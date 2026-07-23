# Перенос данных из MySQL / MariaDB в {{ ydb-short-name }} с помощью Федеративные запросы

Пошаговый рецепт — перенос данных из **MySQL / MariaDB** в {{ ydb-short-name }} с помощью [Федеративные запросы](../../../../concepts/query_execution/federated_query/import_and_export.md).

## Подготовка {#prerequisites}


{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (MySQL / MariaDB)

```bash
mysql -h mysql-host -u user -p mydb -e "SELECT 1"
```

## Пошаговая инструкция {#steps}

Подробнее: [MySQL как внешний источник](../../../../concepts/query_execution/federated_query/mysql.md), [импорт](../../../../concepts/query_execution/federated_query/import_and_export.md).

```yql
CREATE SECRET mysql_password WITH (value = "secret");

CREATE EXTERNAL DATA SOURCE mysql_src WITH (
    SOURCE_TYPE="MySQL",
    LOCATION="mysql-host:3306",
    DATABASE_NAME="mydb",
    AUTH_METHOD="BASIC",
    LOGIN="user",
    PASSWORD_SECRET_PATH="mysql_password"
);

CREATE TABLE `mydb/orders` (
    `id` Int64,
    `amount` Double,
    PRIMARY KEY (`id`)
);

UPSERT INTO `mydb/orders`
SELECT * FROM mysql_src.orders;
```

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/orders"
```

Сравните с источником:

```bash
mysql -h mysql-host -u user -p mydb -e "SELECT COUNT(*) FROM orders"
```
