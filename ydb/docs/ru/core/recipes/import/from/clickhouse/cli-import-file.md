# Перенос данных из ClickHouse в {{ ydb-short-name }} с помощью CLI import file

Пошаговый рецепт: **ClickHouse** → {{ ydb-short-name }} через [CLI import file](../../../../reference/ydb-cli/export-import/import-file.md).

## Подготовка {#prerequisites}


{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (ClickHouse)

```bash
clickhouse-client --host ch-host --query "SELECT 1"
```

## Пошаговая инструкция {#steps}

Подробнее: [import file](../../../../reference/ydb-cli/export-import/import-file.md).

### Шаг 1. Таблица в {{ ydb-short-name }}

```yql
CREATE TABLE `mydb/events` (
    `event_date` Date,
    `user_id` Int64,
    `value` Double,
    PRIMARY KEY (`event_date`, `user_id`)
);
```

### Шаг 2. Экспорт из ClickHouse

CSV:

```bash
clickhouse-client --host ch-host --query \
  "SELECT event_date, user_id, value FROM mydb.events FORMAT CSVWithNames" \
  > /tmp/events.csv
```

Parquet (рекомендуется для больших объёмов):

```bash
clickhouse-client --host ch-host --query \
  "SELECT event_date, user_id, value FROM mydb.events FORMAT Parquet" \
  > /tmp/events.parquet
```

### Шаг 3. Импорт в {{ ydb-short-name }}

```bash
# CSV
ydb -e grpc://localhost:2136 -d /local import file csv \
  --path mydb/events --header /tmp/events.csv

# Parquet
ydb -e grpc://localhost:2136 -d /local import file parquet \
  --path mydb/events /tmp/events.parquet
```

При импорте Parquet учитывайте [маппинг типов Arrow/YQL](../../../../concepts/query_execution/federated_query/s3/arrow_types_mapping.md).

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/events"
```

Сравните с источником:

```bash
clickhouse-client --host ch-host --query "SELECT count() FROM mydb.events"
```
