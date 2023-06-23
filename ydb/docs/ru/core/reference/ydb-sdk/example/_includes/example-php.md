# Приложение на PHP

На этой странице подробно разбирается код тестового приложения, доступного в составе [PHP SDK](https://github.com/ydb-platform/ydb-php-sdk) {{ ydb-short-name }}.

{% include [addition.md](auxilary/addition.md) %}

{% include [init.md](steps/01_init.md) %}

Фрагмент кода приложения для инициализации драйвера:

```php
<?php

use YandexCloud\Ydb\Ydb;

$config = [
    // ...
];

$ydb = new Ydb($config);

```

Фрагмент кода приложения для создания сессии:

```php
// obtaining the Table service
$table = $ydb->table();

// obtaining a session
$session = $table->session();
```


