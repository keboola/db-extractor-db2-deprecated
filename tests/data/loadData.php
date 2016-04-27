<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 15/02/16
 * Time: 16:44
 */

$pdo = new \PDO(
    "odbc:DRIVER={IBM DB2 ODBC DRIVER};DATABASE=SAMPLE;HOSTNAME=db2;PORT=50000;PROTOCOL=TCPIP;",
    'db2inst1',
    'db2inst1'
);

$result = $pdo->exec("
    import from 'escaping.csv' of csv replace into escaping
");

var_dump($result);

var_dump($pdo->errorCode());

var_dump($pdo->errorInfo());

