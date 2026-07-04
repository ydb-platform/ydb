# Перенос данных из Microsoft SQL Server в {{ ydb-short-name }} с помощью Федеративные запросы

Пошаговый рецепт: **Microsoft SQL Server** → {{ ydb-short-name }} через [Федеративные запросы](../../tools/federated-queries.md).

## Подготовка {#prerequisites}

{% include notitle [Федеративные запросы](../../_includes/tools/federated-queries-about.md) %}

{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (Microsoft SQL Server)

```bash
# sqlcmd -S mssql-host -U user -P password -Q "SELECT 1"
```

## Пошаговая инструкция {#steps}

Подробнее: [Microsoft SQL Server](../../../../concepts/query_execution/federated_query/ms_sql_server.md).

```yql
CREATE SECRET mssql_password WITH (value = "secret");

CREATE EXTERNAL DATA SOURCE mssql_src WITH (
    SOURCE_TYPE="MsSQLServer",
    LOCATION="mssql-host:1433",
    DATABASE_NAME="mydb",
    AUTH_METHOD="BASIC",
    LOGIN="user",
    PASSWORD_SECRET_PATH="mssql_password",
    USE_TLS="TRUE"
);

CREATE TABLE `mydb/sales` (
    `id` Int64,
    `region` Text,
    PRIMARY KEY (`id`)
);

UPSERT INTO `mydb/sales`
SELECT * FROM mssql_src.sales;
```

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/sales"
```
