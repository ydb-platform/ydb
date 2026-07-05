# Перенос данных из ClickHouse в {{ ydb-short-name }} с помощью dbt

Пошаговый рецепт — перенос данных из **ClickHouse** в {{ ydb-short-name }} с помощью [dbt](../../../../integrations/migration/dbt.md).

## Подготовка {#prerequisites}


{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (ClickHouse)

```bash
clickhouse-client --host ch-host --query "SELECT 1"
```

## Пошаговая инструкция {#steps}

Подробнее: [dbt](../../../../integrations/migration/dbt.md).

### Шаг 1. Создайте External Data Source

См. [Федеративные запросы](federated-queries.md) (источник `ch_src`).

### Шаг 2. Установите dbt-ydb и создайте модель

```bash
pip install dbt-ydb
```

`models/events.sql`:

```sql
{{ config(materialized='table', primary_key='event_date, user_id', store_type='column') }}
SELECT event_date, user_id, value FROM ch_src.events
```

```bash
dbt run --select events
```

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/events"
```

Сравните с источником:

```bash
clickhouse-client --host ch-host --query "SELECT count() FROM mydb.events"
```
