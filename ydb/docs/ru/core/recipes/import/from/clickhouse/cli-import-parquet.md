# Перенос данных из ClickHouse в {{ ydb-short-name }} через промежуточный Parquet с помощью YDB CLI

Пошаговый рецепт — перенос данных из **ClickHouse** в {{ ydb-short-name }} через промежуточный Parquet, используя [YDB CLI](../../../../reference/ydb-cli/export-import/import-file.md).

Если промежуточный файл не нужен, рассмотрите [Spark](spark.md) или прямой JDBC-импорт через [ydb-importer](ydb-importer.md).

## Подготовка {#prerequisites}

{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (ClickHouse)

```bash
clickhouse-client --host ch-host --query "SELECT 1"
```

## Пошаговая инструкция {#steps}

ClickHouse нативно поддерживает экспорт в Parquet — это предпочтительный промежуточный формат для больших объёмов.

Подробнее о команде: [import file в YDB CLI](../../../../reference/ydb-cli/export-import/import-file.md).

### Шаг 1. Таблица в {{ ydb-short-name }}

```yql
CREATE TABLE `mydb/events` (
    `event_date` Date,
    `user_id` Int64,
    `value` Double,
    PRIMARY KEY (`event_date`, `user_id`)
);
```

### Шаг 2. Экспорт из ClickHouse в Parquet

```bash
clickhouse-client --host ch-host --query \
  "SELECT event_date, user_id, value FROM mydb.events FORMAT Parquet" \
  > /tmp/events.parquet
```

Для нескольких файлов разбейте выборку по партициям (`WHERE toYYYYMM(event_date) = …`) и передайте все пути одной команде `import file parquet`.

### Шаг 3. Импорт в {{ ydb-short-name }}

```bash
ydb -e grpc://localhost:2136 -d /local import file parquet \
  --path mydb/events \
  /tmp/events.parquet
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
