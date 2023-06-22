# App in PHP

This page contains a detailed description of the code of a test app that is available as part of the {{ ydb-short-name }} [PHP SDK](https://github.com/ydb-platform/ydb-php-sdk).

{% include [addition.md](auxilary/addition.md) %}

{% include [init.md](steps/01_init.md) %}

App code snippet for driver initialization:

```php
<?php

use YdbPlatform\Ydb\Ydb;

$config = [
    // ...
];

$ydb = new Ydb($config);
```

App code snippet for creating a session:

```php
// obtaining the Table service
$table = $ydb->table();

// obtaining a session
$session = $table->session();
```

