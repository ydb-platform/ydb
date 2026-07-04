# Перенос данных из Greenplum в {{ ydb-short-name }} с помощью Федеративные запросы

Узлы {{ ydb-short-name }} читают данные из Greenplum через External Data Source и записывают в локальную таблицу одним YQL-запросом.

Подробнее про инструмент: [импорт через федеративные запросы](../../../concepts/query_execution/federated_query/import_and_export.md).

## Системные требования и подготовка {#prerequisites}

| Компонент | Требование |
| --- | --- |
| Кластер {{ ydb-short-name }} | Рабочий endpoint (например, `grpc://localhost:2136`, база `/local`) |
| Greenplum | Сетевой или файловый доступ, учётная запись с правами `SELECT` |
| Кодировка | UTF-8 для текстовых данных |

### Установка {{ ydb-short-name }} CLI

```bash
curl -sSL https://install.ydb.tech/cli | bash
ydb version
```

Подробнее: [Установка YDB CLI](../../../reference/ydb-cli/install.md).

### Проверка подключения к {{ ydb-short-name }}

```bash
ydb -e grpc://localhost:2136 -d /local scheme ls
```

### Проверка доступа к источнику (Greenplum)

```bash
psql "postgresql://gpadmin:password@gp-master:5432/tpch" -c "SELECT 1"
```

---

## Пошаговая инструкция {#steps}

Подробнее: [Greenplum](../../../concepts/query_execution/federated_query/greenplum.md).

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

---

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/lineitem"
```

Сравните с источником:

```bash
psql "postgresql://gpadmin:password@gp-master:5432/tpch" -c "SELECT COUNT(*) FROM lineitem"
```
