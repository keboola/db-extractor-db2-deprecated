<?php

namespace Keboola\DbExtractor;

use Keboola\Csv\CsvFile;
use Keboola\DbExtractor\Test\ExtractorTest;
use Symfony\Component\Process\Process;

class DB2ApplicationTest extends ExtractorTest
{
    public function testTestConnectionAction()
    {
        $config = $this->getConfig('db2');
        $config['action'] = 'testConnection';
        @unlink($this->dataDir . '/config.json');
        file_put_contents(
            $this->dataDir . '/config.json',
            json_encode($config)
        );

        $process = new Process('php ' . ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        var_dump($process->getOutput());
        var_dump($process->getErrorOutput());

        $this->assertEquals(0, $process->getExitCode());
        $this->assertJson($process->getOutput());
        $this->assertEquals("", $process->getErrorOutput());
    }

    public function testTestSSHConnectionAction()
    {
        $config = $this->getConfig('db2');

        $config['action'] = 'testConnection';
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
            'localPort' => '15213',
        ];

        @unlink($this->dataDir . '/config.json');
        file_put_contents($this->dataDir . '/config.json', json_encode($config));

        $process = new Process('php ' . ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();
        $this->assertEquals(0, $process->getExitCode());
        $this->assertJson($process->getOutput());
        $this->assertEquals("", $process->getErrorOutput());
    }

    public function testRunAction()
    {
        $outputCsvFile = $this->dataDir . '/out/tables/in.c-main.db2projact.csv';
        @unlink($outputCsvFile);

        $config = $this->getConfig('db2');
        @unlink($this->dataDir . '/config.json');
        file_put_contents($this->dataDir . '/config.json', json_encode($config));

        $csv1 = new CsvFile($this->dataDir . '/projact.csv');

        // run entrypoint
        $process = new Process('php ' . ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        var_dump($process->getOutput());

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());

        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($this->dataDir . '/out/tables/in.c-main.db2projact.csv.manifest');
        $this->assertFileEquals((string) $csv1, $outputCsvFile);
    }

    public function testSSHRunAction()
    {
        $outputCsvFile = $this->dataDir . '/out/tables/in.c-main.db2projact.csv';
        @unlink($outputCsvFile);

        $config = $this->getConfig('db2');
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
            'localPort' => '15214',
        ];
        @unlink($this->dataDir . '/config.json');
        file_put_contents($this->dataDir . '/config.json', json_encode($config));

        $csv1 = new CsvFile($this->dataDir . '/projact.csv');

        // run entrypoint
        $process = new Process('php ' . ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        var_dump($process->getOutput());

        $this->assertEquals("", $process->getErrorOutput());
        $this->assertEquals(0, $process->getExitCode());

        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($this->dataDir . '/out/tables/in.c-main.db2projact.csv.manifest');
        $this->assertFileEquals((string) $csv1, $outputCsvFile);

        $manifest = file_get_contents($this->dataDir . '/out/tables/in.c-main.db2projact.csv.manifest');
        $manifest = json_decode($manifest, true);

        $this->assertArrayHasKey('destination', $manifest);
        $this->assertArrayHasKey('incremental', $manifest);
    }

    protected function getConfig($driver)
    {
        $config = json_decode(
            file_get_contents($this->dataDir . '/' .$driver . '/config.json'),
            true
        )
        ;
        $config['parameters']['data_dir'] = $this->dataDir;

        $config['parameters']['db']['user'] = $this->getEnv($driver, 'DB_USER', true);
        $config['parameters']['db']['#password'] = $this->getEnv($driver, 'DB_PASSWORD', true);
        $config['parameters']['db']['host'] = $this->getEnv($driver, 'DB_HOST');
        $config['parameters']['db']['port'] = $this->getEnv($driver, 'DB_PORT');
        $config['parameters']['db']['database'] = $this->getEnv($driver, 'DB_DATABASE');

        return $config;
    }
}