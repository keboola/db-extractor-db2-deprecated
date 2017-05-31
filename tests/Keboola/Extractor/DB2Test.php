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
		foreach ($result['tables'] as $i => $table) {
            $this->assertArrayHasKey('name', $table);
            $this->assertArrayHasKey('columns', $table);
            if (count($table['columns']) > 0) { // sample DB has a table with no columns
                foreach ($table['columns'] as $j => $column) {
                    $this->assertArrayHasKey('name', $column);
                    $this->assertArrayHasKey('type', $column);
                    $this->assertArrayHasKey('length', $column);
                    $this->assertArrayHasKey('nullable', $column);
                    $this->assertArrayHasKey('default', $column);
                    $this->assertArrayHasKey('ordinalPosition', $column);
                }
                if ($table['name'] === 'ESCAPING') {
                    foreach ($table['columns'] as $j => $column) {
                        $this->assertArrayHasKey('indexed', $column);
                        $this->assertTrue($column['indexed']);
                        $this->assertArrayHasKey('primaryKey', $column);
                        $this->assertTrue($column['primaryKey']);
                        $this->assertArrayHasKey('uniqueKey', $column);
                        $this->assertFalse($column['primaryKey']);

                        $this->assertEquals("VARCHAR", $column['type']);
                        $this->assertEquals("255", $column['length']);
                        $this->assertNull($column['default']);
                        $this->assertFalse($column['nullable']);
                    }
                }
                if ($table['name'] === 'MULTIPK') {
                    foreach ($table['columns'] as $j => $column) {
                        $this->assertArrayHasKey('foreignKeyRef', $column);
                        $this->assertEquals('PK1', $column['foreignKeyRef']);
                        $this->assertArrayHasKey('foreignKeyRefTable', $column);
                        $this->assertEquals('ESCAPING', $column['foreignKeyRefTable']);

                        $this->assertEquals("VARCHAR", $column['type']);
                        $this->assertEquals("255", $column['length']);
                        $this->assertNull($column['default']);
                        $this->assertFalse($column['nullable']);
                    }
                }
            }
        }
	}
}
