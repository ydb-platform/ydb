# Перенос данных из Greenplum в {{ ydb-short-name }} с помощью dbt

Пошаговый рецепт — перенос данных из **Greenplum** в {{ ydb-short-name }} с помощью [dbt](../../../../integrations/migration/dbt.md).

## Подготовка {#prerequisites}


{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (Greenplum)

```bash
psql "postgresql://gpadmin:password@gp-master:5432/tpch" -c "SELECT 1"
```

## Пошаговая инструкция {#steps}

Подробнее: [dbt](../../../../integrations/migration/dbt.md).

### Шаг 1. Создайте External Data Source

См. [Федеративные запросы](federated-queries.md) (источник `gp_src`).

### Шаг 2. Установите dbt-ydb и создайте модель

```bash
pip install dbt-ydb
```

`models/lineitem.sql`:

```sql
{{ config(materialized='table', primary_key='l_orderkey, l_linenumber', store_type='column') }}
SELECT l_orderkey, l_linenumber, l_quantity FROM gp_src.lineitem
```

```bash
dbt run --select lineitem
```

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/lineitem"
```

Сравните с источником:

```bash
psql "postgresql://gpadmin:password@gp-master:5432/tpch" -c "SELECT COUNT(*) FROM lineitem"
```
