# Перенос данных из ClickHouse в {{ ydb-short-name }} с помощью Федеративные запросы

Пошаговый рецепт: **ClickHouse** → {{ ydb-short-name }} через [Федеративные запросы](../../tools/federated-queries.md).

## Подготовка {#prerequisites}

{% include notitle [Федеративные запросы](../../_includes/tools/federated-queries-about.md) %}

{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (ClickHouse)

```bash
clickhouse-client --host ch-host --query "SELECT 1"
```

## Пошаговая инструкция {#steps}

Подробнее: [ClickHouse](../../../../concepts/query_execution/federated_query/clickhouse.md), [импорт](../../../../concepts/query_execution/federated_query/import_and_export.md).

```yql
CREATE SECRET ch_password WITH (value = "secret");

CREATE EXTERNAL DATA SOURCE ch_src WITH (
    SOURCE_TYPE="ClickHouse",
    LOCATION="ch-host:8443",
    DATABASE_NAME="mydb",
    AUTH_METHOD="BASIC",
    LOGIN="default",
    PASSWORD_SECRET_PATH="ch_password",
    USE_TLS="TRUE"
);

CREATE TABLE `mydb/events` (
    `event_date` Date,
    `user_id` Int64,
    `value` Double,
    PRIMARY KEY (`event_date`, `user_id`)
);

UPSERT INTO `mydb/events`
SELECT * FROM ch_src.events;
```

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/events"
```

Сравните с источником:

```bash
clickhouse-client --host ch-host --query "SELECT count() FROM mydb.events"
```
