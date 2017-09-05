<?php

/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 10/12/15
 * Time: 14:25
 */

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Test\ExtractorTest;
use Symfony\Component\Yaml\Yaml;


class DB2Test extends ExtractorTest
{
    /** @var Application */
    protected $app;

    public function setUp()
    {
        $this->app = new Application($this->getConfig());
    }

    public function getConfig($driver = 'db2')
    {
        $config = parent::getConfig('db2');
        $config['parameters']['extractor_class'] = 'DB2';
        return $config;
    }

    public function testRun()
    {
        $result = $this->app->run();
        $expectedCsvFile = ROOT_PATH . '/tests/data/projact.csv';
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv';

        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($outputManifestFile);
        $this->assertEquals(file_get_contents($expectedCsvFile), file_get_contents($outputCsvFile));
    }

    public function testEscaping()
    {
        $expectedCsvFile = ROOT_PATH . '/tests/data/escaping.csv';

        // load data to DB
        $db = $this->getConnection();
        $db->exec("DROP TABLE escaping");
        $db->exec("CREATE TABLE escaping (col1 VARCHAR(255) NOT NULL, col2 VARCHAR(255) NOT NULL)");

        $fh = fopen($expectedCsvFile, 'r+');
        $i = 0;
        while ($row = fgetcsv($fh, null, ",", '"', '\\')) {
            if ($i != 0) {
                $res = $db->exec(sprintf("INSERT INTO escaping VALUES ('%s', '%s')", $row[0], $row[1]));
            }
            $i++;
        }

        $config = $this->getConfig();
        $config['parameters']['tables'][0] = [
            'id' => 0,
            'name' => 'escaping',
            'query' => 'SELECT * FROM DB2INST1.ESCAPING',
            'outputTable' => 'in.c-main.db2escaping',
            'incremental' => false,
            'primaryKey' => null,
            'enabled' => true
        ];

        $this->app = new Application($config);

        $result = $this->app->run();
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($outputManifestFile);
        $this->assertEquals(file_get_contents($expectedCsvFile), file_get_contents($outputCsvFile));
    }

    private function getConnection()
    {
        $config = $this->getConfig()['parameters']['db'];
        $database = $config['database'];
        $user = $config['user'];
        $password = $config['password'];
        $hostname = $config['host'];
        $port = $config['port'];

        $dsn = sprintf(
            "odbc:DRIVER={IBM DB2 ODBC DRIVER};DATABASE=%s;HOSTNAME=%s;PORT=%s;PROTOCOL=TCPIP;",
            $database,
            $hostname,
            $port
        );

        return new \PDO($dsn, $user, $password);
    }

    public function testTestConnection()
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';
        $app = new Application($config);

        $result = $app->run();
        $this->assertEquals('success', $result['status']);
    }

    public function testTestConnectionFailed()
    {
        $config = $this->getConfig();
        $config['parameters']['db']['user'] = 'thisUserDoesNotExist';
        $config['parameters']['db']['password'] = 'wrongPasswordObviously';
        $config['action'] = 'testConnection';
        $app = new Application($config);

        $exception = null;
        try {
            $result = $app->run();
        } catch(UserException $e) {
            $exception = $e;
        }

        $this->assertContains('Connection failed', $exception->getMessage());
    }

    public function testCredentialsWithSSH()
    {
        $config = $this->getConfig();

        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getEnv('mysql', 'DB_SSH_KEY_PRIVATE'),
                'public' => $this->getEnv('mysql', 'DB_SSH_KEY_PUBLIC')
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'remoteHost' => 'db2',
            'remotePort' => $config['parameters']['db']['port'],
            'localPort' => '15211',
        ];

        $config['action'] = 'testConnection';
        unset($config['parameters']['tables']);

        $app = new Application($config);
        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testRunWithSSH()
    {
        $config = $this->getConfig();
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getEnv('mysql', 'DB_SSH_KEY_PRIVATE'),
                'public' => $this->getEnv('mysql', 'DB_SSH_KEY_PUBLIC')
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'remoteHost' => 'db2',
            'remotePort' => $config['parameters']['db']['port'],
            'localPort' => '15212',
        ];

        $app = new Application($config);

        $result = $app->run();

        $expectedCsvFile = ROOT_PATH . '/tests/data/projact.csv';
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($outputManifestFile);
        $this->assertEquals(file_get_contents($expectedCsvFile), file_get_contents($outputCsvFile));
    }

	public function testGetTablesAction()
	{
		$config = $this->getConfig();
		$config['action'] = 'getTables';
		$app = new Application($config);

        // set up a reference for constraint testing
        $conn = $this->getConnection();
        $conn->exec("ALTER TABLE escaping ADD CONSTRAINT pk1 PRIMARY KEY (col1, col2)");
        $conn->exec("DROP TABLE multipk");
        $conn->exec("CREATE TABLE multipk (col1 VARCHAR(255) NOT NULL, col2 VARCHAR(255) NOT NULL)");
        $conn->exec("ALTER TABLE multipk ADD CONSTRAINT fk1 FOREIGN KEY (col1, col2) REFERENCES escaping (col1, col2)");

        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('tables', $result);
        $this->assertEquals('success', $result['status']);
        $this->assertCount(49, $result['tables']);

        $table0 = array (
            'name' => 'ACT',
            'schema' => 'DB2INST1',
            'type' => 'TABLE',
            'columns' =>
                array (
                    0 =>
                        array (
                            'name' => 'ACTNO',
                            'type' => 'SMALLINT',
                            'nullable' => false,
                            'default' => NULL,
                            'length' => '2',
                            'ordinalPosition' => '0',
                            'indexed' => true,
                            'primaryKey' => true,
                            'uniqueKey' => false,
                            'foreignKeyRefTable' => 'ACT',
                            'foreignKeyRef' => 'PK_ACT',
                        ),
                    1 =>
                        array (
                            'name' => 'ACTNO',
                            'type' => 'SMALLINT',
                            'nullable' => false,
                            'default' => NULL,
                            'length' => '2',
                            'ordinalPosition' => '0',
                            'indexed' => true,
                            'primaryKey' => false,
                            'uniqueKey' => true,
                            'foreignKeyRefTable' => 'ACT',
                            'foreignKeyRef' => 'PK_ACT',
                        ),
                    2 =>
                        array (
                            'name' => 'ACTKWD',
                            'type' => 'CHARACTER',
                            'nullable' => false,
                            'default' => NULL,
                            'length' => '6',
                            'ordinalPosition' => '1',
                            'indexed' => true,
                            'primaryKey' => false,
                            'uniqueKey' => true,
                        ),
                    3 =>
                        array (
                            'name' => 'ACTDESC',
                            'type' => 'VARCHAR',
                            'nullable' => false,
                            'default' => NULL,
                            'length' => '20',
                            'ordinalPosition' => '2',
                            'primaryKey' => false
                        ),
                ),

        );
        $this->assertEquals($table0, $result['tables'][0]);

        $table1 = array (
            'name' => 'ADEFUSR',
            'schema' => 'DB2INST1',
            'type' => 'S',
            'columns' =>
                array (
                    0 =>
                        array (
                            'name' => 'WORKDEPT',
                            'type' => 'CHARACTER',
                            'nullable' => true,
                            'default' => NULL,
                            'length' => '3',
                            'ordinalPosition' => '0',
                            'primaryKey' => false
                        ),
                    1 =>
                        array (
                            'name' => 'NO_OF_EMPLOYEES',
                            'type' => 'INTEGER',
                            'nullable' => false,
                            'default' => NULL,
                            'length' => '4',
                            'ordinalPosition' => '1',
                            'primaryKey' => false
                        ),
                ),
        );

        $this->assertEquals($table1, $result['tables'][1]);
    }

    public function testSimpleQueryRun()
    {
        $config = $this->getConfig();

        unset($config['parameters']['tables'][0]);

        $app = new Application($config);

        $result = $app->run();

        $expectedCsvFile = ROOT_PATH . '/tests/data/department.csv';
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($outputManifestFile);
        $this->assertEquals(file_get_contents($expectedCsvFile), file_get_contents($outputCsvFile));
    }

    public function testManifestMetadata()
    {
        $config = $this->getConfig();

        // use just the 1 table with table/columns
        unset($config['parameters']['tables'][0]);

        $app = new Application($config);

        $result = $app->run();

        $outputManifest = Yaml::parse(
            file_get_contents($this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest')
        );

        $this->assertArrayHasKey('destination', $outputManifest);
        $this->assertArrayHasKey('incremental', $outputManifest);
        $this->assertArrayHasKey('metadata', $outputManifest);

        $expectedTableMetadata = array (
            0 =>
                array (
                    'key' => 'KBC.name',
                    'value' => 'DEPARTMENT',
                ),
            1 =>
                array (
                    'key' => 'KBC.schema',
                    'value' => 'DB2INST1',
                ),
            2 =>
                array (
                    'key' => 'KBC.type',
                    'value' => 'TABLE',
                ),
        );
        $this->assertEquals($expectedTableMetadata, $outputManifest['metadata']);

        $this->assertArrayHasKey('column_metadata', $outputManifest);
        $this->assertCount(5, $outputManifest['column_metadata']);

        $expectedColumnMetadata = array (
            'DEPTNO' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'CHARACTER',
                        ),
                    1 =>
                        array (
                            'key' => 'KBC.datatype.nullable',
                            'value' => false,
                        ),
                    2 =>
                        array (
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'STRING',
                        ),
                    3 =>
                        array (
                            'key' => 'KBC.datatype.length',
                            'value' => '3',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => true,
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '0',
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.indexed',
                            'value' => true,
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.uniqueKey',
                            'value' => false,
                        ),
                ),
            'DEPTNAME' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'VARCHAR',
                        ),
                    1 =>
                        array (
                            'key' => 'KBC.datatype.nullable',
                            'value' => false,
                        ),
                    2 =>
                        array (
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'STRING',
                        ),
                    3 =>
                        array (
                            'key' => 'KBC.datatype.length',
                            'value' => '36',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '1',
                        ),
                ),
            'MGRNO' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'CHARACTER',
                        ),
                    1 =>
                        array (
                            'key' => 'KBC.datatype.nullable',
                            'value' => true,
                        ),
                    2 =>
                        array (
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'STRING',
                        ),
                    3 =>
                        array (
                            'key' => 'KBC.datatype.length',
                            'value' => '6',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '2',
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.indexed',
                            'value' => true,
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.uniqueKey',
                            'value' => false,
                        ),
                    8 =>
                        array (
                            'key' => 'KBC.foreignKeyRefTable',
                            'value' => 'EMPLOYEE',
                        ),
                    9 =>
                        array (
                            'key' => 'KBC.foreignKeyRef',
                            'value' => 'PK_EMPLOYEE',
                        ),
                ),
            'ADMRDEPT' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'CHARACTER',
                        ),
                    1 =>
                        array (
                            'key' => 'KBC.datatype.nullable',
                            'value' => false,
                        ),
                    2 =>
                        array (
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'STRING',
                        ),
                    3 =>
                        array (
                            'key' => 'KBC.datatype.length',
                            'value' => '3',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '3',
                        ),
                    6 =>
                        array (
                            'key' => 'KBC.indexed',
                            'value' => true,
                        ),
                    7 =>
                        array (
                            'key' => 'KBC.uniqueKey',
                            'value' => false,
                        ),
                    8 =>
                        array (
                            'key' => 'KBC.foreignKeyRefTable',
                            'value' => 'DEPARTMENT',
                        ),
                    9 =>
                        array (
                            'key' => 'KBC.foreignKeyRef',
                            'value' => 'PK_DEPARTMENT',
                        ),
                ),
            'LOCATION' =>
                array (
                    0 =>
                        array (
                            'key' => 'KBC.datatype.type',
                            'value' => 'CHARACTER',
                        ),
                    1 =>
                        array (
                            'key' => 'KBC.datatype.nullable',
                            'value' => true,
                        ),
                    2 =>
                        array (
                            'key' => 'KBC.datatype.basetype',
                            'value' => 'STRING',
                        ),
                    3 =>
                        array (
                            'key' => 'KBC.datatype.length',
                            'value' => '16',
                        ),
                    4 =>
                        array (
                            'key' => 'KBC.primaryKey',
                            'value' => false,
                        ),
                    5 =>
                        array (
                            'key' => 'KBC.ordinalPosition',
                            'value' => '4',
                        ),
                ),
        );
        $this->assertEquals($expectedColumnMetadata, $outputManifest['column_metadata']);
    }
}
