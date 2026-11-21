<?php

use YdbPlatform\Ydb\Exception;
use YdbPlatform\Ydb\Ydb;

require __DIR__ . '/vendor/autoload.php';

$driver = new Ydb([
    'endpoint' => getenv('YDB_ENDPOINT'),
    'database' => getenv('YDB_DATABASE'),
    'discovery' => true,
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
