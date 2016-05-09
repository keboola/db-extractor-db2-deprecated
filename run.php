<?php

use Keboola\DbExtractor\Application;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Symfony\Component\Yaml\Yaml;

define('APP_NAME', 'ex-db-db2');
define('ROOT_PATH', __DIR__);

require_once(dirname(__FILE__) . "/vendor/keboola/db-extractor-common/bootstrap.php");

$logger = new \Keboola\DbExtractor\Logger(APP_NAME);

try {
    $arguments = getopt("d::", ["data::"]);
    if (!isset($arguments["data"])) {
        throw new UserException('Data folder not set.');
    }

    $config = Yaml::parse(file_get_contents($arguments["data"] . "/config.yml"));
    $config['parameters']['dataDir'] = $arguments['data'];
    $config['parameters']['extractor_class'] = 'DB2';

    $app = new Application($config);
    echo json_encode($app->run());

} catch(UserException $e) {
    $logger->log('error', $e->getMessage(), (array) $e->getData());
    exit(1);
} catch(ApplicationException $e) {
    $logger->log('error', $e->getMessage(), (array) $e->getData());
    exit($e->getCode() > 1 ? $e->getCode(): 2);
} catch(\Exception $e) {
    $logger->log('error', $e->getMessage(), [
        'errFile' => $e->getFile(),
        'errLine' => $e->getLine(),
        'trace' => $e->getTrace()
    ]);
    exit(2);
}

exit(0);
