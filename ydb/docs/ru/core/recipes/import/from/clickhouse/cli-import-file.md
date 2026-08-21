# Перенос данных из ClickHouse в {{ ydb-short-name }} через промежуточный CSV с помощью YDB CLI

Пошаговый рецепт — перенос данных из **ClickHouse** в {{ ydb-short-name }} через промежуточный CSV, используя [YDB CLI](../../../../reference/ydb-cli/export-import/import-file.md).

## Подготовка {#prerequisites}


{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (ClickHouse)

```bash
clickhouse-client --host ch-host --query "SELECT 1"
```

## Пошаговая инструкция {#steps}

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

### Шаг 2. Экспорт из ClickHouse в CSV

```bash
clickhouse-client --host ch-host --query \
  "SELECT event_date, user_id, value FROM mydb.events FORMAT CSVWithNames" \
  > /tmp/events.csv
```

Для больших объёмов предпочтительнее [Parquet](cli-import-parquet.md).

### Шаг 3. Импорт в {{ ydb-short-name }}

```bash
ydb -e grpc://localhost:2136 -d /local import file csv \
  --path mydb/events --header /tmp/events.csv
```

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/events"
```

Сравните с источником:

```bash
clickhouse-client --host ch-host --query "SELECT count() FROM mydb.events"
```
