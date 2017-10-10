<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 10/02/16
 * Time: 17:49
 */

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;

class DB2 extends Extractor
{
    private $dbConfig;

    public function createConnection($dbParams)
    {
        $this->dbConfig = $dbParams;

        // check params
        foreach (['host', 'database', 'user', 'password'] as $r) {
            if (!isset($dbParams[$r])) {
                throw new UserException(sprintf("Parameter %s is missing.", $r));
            }
        }

        // convert errors to PDOExceptions
        $options = [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION
        ];

        $port = isset($dbParams['port']) ? $dbParams['port'] : '50000';

        $dsn = sprintf(
            "odbc:DRIVER={IBM DB2 ODBC DRIVER};HOSTNAME=%s;PORT=%s;DATABASE=%s;PROTOCOL=TCPIP;",
            $dbParams['host'],
            $port,
            $dbParams['database']
        );

        $pdo = new \PDO($dsn, $dbParams['user'], $dbParams['password'], $options);

        return $pdo;
    }

    /**
     * @param array $table
     * @return $outputTable output table name
     * @throws ApplicationException
     * @throws UserException
     * @throws \Keboola\Csv\Exception
     */
    public function export(array $table)
    {
        if (empty($table['outputTable'])) {
            throw new UserException("Missing attribute 'outputTable'");
        }
        $outputTable = $table['outputTable'];

        if (empty($table['query'])) {
            $query = $this->simpleQuery($table['table'], $table['columns']);
        } else {
            $query = $table['query'];
        }

        $this->logger->info("Exporting to " . $outputTable);
        $csv = $this->createOutputCsv($outputTable);

        // write header and first line
        try {

            $stmt = $this->db->prepare($query);
            $stmt->execute();

            $resultRow = $stmt->fetch(\PDO::FETCH_ASSOC);

            if (is_array($resultRow) && !empty($resultRow)) {
                $csv->writeRow(array_keys($resultRow));
                $csv->writeRow($resultRow);

                // write the rest
                while ($resultRow = $stmt->fetch(\PDO::FETCH_ASSOC)) {
                    $csv->writeRow($resultRow);
                }
            } else {
                $this->logger->warning("Query returned empty result. Nothing was imported.");
            }
        } catch (\PDOException $e) {
            throw new UserException("DB query failed: " . $e->getMessage(), 0, $e);
        }

        if ($this->createManifest($table) === false) {
            throw new ApplicationException("Unable to create manifest", 0, null, [
                'table' => $table
            ]);
        }

        return $outputTable;
    }

    public function testConnection()
    {
        $this->db->query('SELECT 1 FROM sysibm.sysdummy1');
    }

    public function getTables(array $tables = null)
    {
        $sql = "SELECT * FROM SYSCAT.TABLES WHERE OWNERTYPE = 'U'";
        
        if (!is_null($tables) && count($tables) > 0) {
            $sql .= sprintf(
                " AND TABNAME IN ('%s') AND TABSCHEMA IN ('%s')",
                implode("','", array_map(function ($table) {
                    return $table['tableName'];
                }, $tables)),
                implode("','", array_map(function ($table) {
                    return $table['schema'];
                }, $tables))
            );
        }
        $sql .= " ORDER BY TABNAME";

        $res = $this->db->query($sql);
        $arr = $res->fetchAll(\PDO::FETCH_ASSOC);

        $output = [];
        $tableNameArray = [];
        foreach ($arr as $table) {
            $tableNameArray[] = $table['TABNAME'];
            $tableDefs[$table['TABNAME']] = [
                'name' => $table['TABNAME'],
                'schema' => (isset($table['TABSCHEMA'])) ? $table['TABSCHEMA'] : null
            ];
            switch ($table['TYPE']) {
                case 'T':
                case 'U':
                    $tableDefs[$table['TABNAME']]['type'] = 'TABLE';
                    break;
                case 'V':
                case 'W':
                    $tableDefs[$table['TABNAME']]['type'] = 'VIEW';
                    break;
                default:
                    $tableDefs[$table['TABNAME']]['type'] = $table['TYPE'];
            }
        }

        $sql = sprintf(
            "SELECT COLS.*, IDXCOLS.INDEXTYPE, IDXCOLS.UNIQUERULE, REFCOLS.REFKEYNAME, REFCOLS.REFTABNAME FROM SYSCAT.COLUMNS AS COLS 
            LEFT OUTER JOIN (
                SELECT ICU.COLNAME, IDX.TABNAME, IDX.INDEXTYPE, IDX.UNIQUERULE FROM SYSCAT.INDEXCOLUSE AS ICU
                JOIN SYSCAT.INDEXES AS IDX 
                ON ICU.INDNAME = IDX.INDNAME AND SUBSTR(IDX.INDEXTYPE,1,1) != 'X'
            ) AS IDXCOLS ON COLS.TABNAME = IDXCOLS.TABNAME AND COLS.COLNAME = IDXCOLS.COLNAME
            LEFT OUTER JOIN (
                SELECT KCU.COLNAME, REF.TABNAME, REF.REFKEYNAME, REF.REFTABNAME FROM SYSCAT.KEYCOLUSE AS KCU
                JOIN SYSCAT.REFERENCES AS REF 
                ON KCU.CONSTNAME = REF.CONSTNAME
            ) AS REFCOLS ON COLS.TABNAME = REFCOLS.TABNAME AND COLS.COLNAME = REFCOLS.COLNAME 
            WHERE COLS.TABNAME IN (%s) ORDER BY COLS.TABSCHEMA, COLS.TABNAME, COLS.COLNO",
            implode(', ', array_map(function ($tableName) {
                return "'" . $tableName . "'";
            }, $tableNameArray))
        );

        $res = $this->db->query($sql);

        $arr = $res->fetchAll();

        foreach ($arr as $i => $column) {

            $length = $column['LENGTH'];
            if ($column['SCALE'] != 0 && $column['TYPENAME'] === 'DECIMAL') {
                $length .= "," . $column['SCALE'];
            }
            if (!array_key_exists('columns', $tableDefs[$column['TABNAME']])) {
                $tableDefs[$column['TABNAME']]['columns'] = [];
            }
            $tableDefs[$column['TABNAME']]['columns'][$column['COLNO']] = [
                "name" => $column['COLNAME'],
                "type" => $column['TYPENAME'],
                "nullable" => ($column['NULLS'] === 'N') ? false : true,
                "default" => $column['DEFAULT'],
                "length" => $length,
                "primaryKey" => ($column['UNIQUERULE'] === 'P') ? true : false,
                "ordinalPosition" => $column['COLNO'],
            ];
            if (!is_null($column['INDEXTYPE'])) {
                $tableDefs[$column['TABNAME']]['columns'][$column['COLNO']]['indexed'] = true;
                $tableDefs[$column['TABNAME']]['columns'][$column['COLNO']]['uniqueKey'] = ($column['UNIQUERULE'] === 'U') ? true : false;
            }
            if (!is_null($column['REFKEYNAME'])) {
                $tableDefs[$column['TABNAME']]['columns'][$column['COLNO']]['foreignKeyRefTable'] = $column['REFTABNAME'];
                $tableDefs[$column['TABNAME']]['columns'][$column['COLNO']]['foreignKeyRef'] = $column['REFKEYNAME'];
            }
        }
        return array_values($tableDefs);
    }

    protected function describeTable(array $table)
    {
        // Deprecated
        return null;
    }

    public function simpleQuery(array $table, array $columns = array())
    {
        if (count($columns) > 0) {
            return sprintf("SELECT %s FROM %s.%s",
                implode(', ', array_map(function ($column) {
                    return $this->quote($column);
                }, $columns)),
                $this->quote($table['schema']),
                $this->quote($table['tableName'])
            );
        } else {
            return sprintf(
                "SELECT * FROM %s.%s",
                $this->quote($table['schema']),
                $this->quote($table['tableName'])
            );
        }
    }

    private function quote($obj) {
        return "\"{$obj}\"";
    }
}
