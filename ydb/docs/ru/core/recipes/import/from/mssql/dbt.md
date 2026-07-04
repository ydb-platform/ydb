# Перенос данных из Microsoft SQL Server в {{ ydb-short-name }} с помощью dbt

Пошаговый рецепт: **Microsoft SQL Server** → {{ ydb-short-name }} через [dbt](../../../../integrations/migration/dbt.md).

## Подготовка {#prerequisites}


{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (Microsoft SQL Server)

```bash
# sqlcmd -S mssql-host -U user -P password -Q "SELECT 1"
```

## Пошаговая инструкция {#steps}

Подробнее: [dbt](../../../../integrations/migration/dbt.md).

### Шаг 1. Создайте External Data Source

См. [Федеративные запросы](federated-queries.md) (источник `mssql_src`).

### Шаг 2. Установите dbt-ydb и создайте модель

```bash
pip install dbt-ydb
```

`models/sales.sql`:

```sql
{{ config(materialized='table', primary_key='id') }}
SELECT id, region FROM mssql_src.sales
```

```bash
dbt run --select sales
```

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/sales"
```
