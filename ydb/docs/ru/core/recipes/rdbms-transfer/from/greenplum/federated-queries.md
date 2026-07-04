# Перенос данных из Greenplum в {{ ydb-short-name }} с помощью Федеративные запросы

Пошаговый рецепт: **Greenplum** → {{ ydb-short-name }} через [Федеративные запросы](../../tools/federated-queries.md).

## Подготовка {#prerequisites}

{% include notitle [Федеративные запросы](../../_includes/tools/federated-queries-about.md) %}

{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (Greenplum)

```bash
psql "postgresql://gpadmin:password@gp-master:5432/tpch" -c "SELECT 1"
```

## Пошаговая инструкция {#steps}

Подробнее: [Greenplum](../../../../concepts/query_execution/federated_query/greenplum.md).

```yql
CREATE SECRET gp_password WITH (value = "secret");

CREATE EXTERNAL DATA SOURCE gp_src WITH (
    SOURCE_TYPE="Greenplum",
    LOCATION="gp-master:5432",
    DATABASE_NAME="tpch",
    AUTH_METHOD="BASIC",
    LOGIN="gpadmin",
    PASSWORD_SECRET_PATH="gp_password",
    PROTOCOL="NATIVE",
    SCHEMA="public"
);

CREATE TABLE `mydb/lineitem` (
    `l_orderkey` Int64,
    `l_linenumber` Int32,
    `l_quantity` Double,
    PRIMARY KEY (`l_orderkey`, `l_linenumber`)
);

UPSERT INTO `mydb/lineitem`
SELECT * FROM gp_src.lineitem;
```

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/lineitem"
```

Сравните с источником:

```bash
psql "postgresql://gpadmin:password@gp-master:5432/tpch" -c "SELECT COUNT(*) FROM lineitem"
```
