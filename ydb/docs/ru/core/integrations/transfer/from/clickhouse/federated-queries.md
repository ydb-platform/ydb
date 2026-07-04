# Перенос данных из ClickHouse в {{ ydb-short-name }} с помощью Федеративные запросы

Узлы {{ ydb-short-name }} читают данные из ClickHouse через External Data Source и записывают в локальную таблицу одним YQL-запросом.

Подробнее про инструмент: [импорт через федеративные запросы](../../../concepts/query_execution/federated_query/import_and_export.md).

## Системные требования и подготовка {#prerequisites}

| Компонент | Требование |
| --- | --- |
| Кластер {{ ydb-short-name }} | Рабочий endpoint (например, `grpc://localhost:2136`, база `/local`) |
| ClickHouse | Сетевой или файловый доступ, учётная запись с правами `SELECT` |
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

### Проверка доступа к источнику (ClickHouse)

```bash
clickhouse-client --host ch-host --query "SELECT 1"
```

---

## Пошаговая инструкция {#steps}

Подробнее: [ClickHouse](../../../concepts/query_execution/federated_query/clickhouse.md), [импорт](../../../concepts/query_execution/federated_query/import_and_export.md).

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

---

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/events"
```

Сравните с источником:

```bash
clickhouse-client --host ch-host --query "SELECT count() FROM mydb.events"
```
