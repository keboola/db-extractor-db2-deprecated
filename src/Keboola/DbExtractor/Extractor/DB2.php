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
            throw new UserException("Missing attribute 'query'");
        }
        $query = $table['query'];

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

    private function replaceNull($row, $value)
    {
        foreach ($row as $k => $v) {
            if ($v === null) {
                $row[$k] = $value;
            }
        }

        return $row;
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
                " AND TABNAME IN ('%s')",
                implode("','", array_map(function ($table) {
                    return $table;
                }, $tables))
            );
        }

        $res = $this->db->query($sql);
        $arr = $res->fetchAll(\PDO::FETCH_ASSOC);

        $output = [];
        foreach ($arr as $table) {
            $output[] = $this->describeTable($table);
        }
        return $output;
    }

    protected function describeTable(array $table)
    {
        $tabledef = [
            'name' => $table['TABNAME'],
            'schema' => (isset($table['TABSCHEMA'])) ? $table['TABSCHEMA'] : null
        ];
        if (isset($table['TYPE'])) {
            switch ($table['TYPE']) {
                case 'T':
                case 'U':
                    $tabledef['type'] = 'TABLE';
                    break;
                case 'V':
                case 'W':
                    $tabledef['type'] = 'VIEW';
                    break;
                default:
                    $tabledef['type'] = $table['TYPE'];
            }
        } else {
            $tabledef['type'] = null;
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
            WHERE COLS.TABNAME = '%s' ORDER BY COLS.COLNO",
            $table['TABNAME']
        );

        $res = $this->db->query($sql);

        $arr = $res->fetchAll();
        $columns = [];
        foreach ($arr as $i => $column) {

            $length = $column['LENGTH'];
            if ($column['SCALE'] != 0 && $column['TYPENAME'] === 'DECIMAL') {
                $length .= "," . $column['SCALE'];
            }

            $columns[$i] = [
                "name" => $column['COLNAME'],
                "type" => $column['TYPENAME'],
                "nullable" => ($column['NULLS'] === 'N') ? false : true,
                "default" => $column['DEFAULT'],
                "length" => $length,
                "ordinalPosition" => $column['COLNO'],
            ];
            if (!is_null($column['INDEXTYPE'])) {
                $columns[$i]['indexed'] = true;
                $columns[$i]['primaryKey'] = ($column['UNIQUERULE'] === 'P') ? true : false;
                $columns[$i]['uniqueKey'] = ($column['UNIQUERULE'] === 'U') ? true : false;
            }
            if (!is_null($column['REFKEYNAME'])) {
                $columns[$i]['foreignKeyRefTable'] = $column['REFTABNAME'];
                $columns[$i]['foreignKeyRef'] = $column['REFKEYNAME'];
            }
        }
        $tabledef['columns'] = $columns;

        return $tabledef;
    }
}
