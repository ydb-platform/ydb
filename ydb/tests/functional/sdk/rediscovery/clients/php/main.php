<?php

use YdbPlatform\Ydb\Exception;
use YdbPlatform\Ydb\Ydb;

require (getenv('YDB_PHP_AUTOLOAD') ?: __DIR__ . '/vendor/autoload.php');

$driver = new Ydb([
    'endpoint' => getenv('YDB_ENDPOINT'),
    'database' => getenv('YDB_DATABASE'),
    'discovery' => true,
    'discoveryLongPoll' => getenv('YDB_LONG_POLL_DISCOVERY') === '1',
    'iam_config' => ['anonymous' => true, 'insecure' => true],
    'grpc' => ['timeout' => 2_000_000],
]);
$scheme = $driver->scheme();

while (true) {
    try {
        $scheme->listDirectory();
        fwrite(STDOUT, "OK\n");
    } catch (Exception $error) {
        fwrite(STDERR, $error->getMessage() . "\n");
    }
    usleep(200_000);
}
