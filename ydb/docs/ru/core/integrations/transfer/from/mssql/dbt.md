# Перенос данных из Microsoft SQL Server в {{ ydb-short-name }} с помощью dbt (dbt-ydb)

dbt материализует SQL-модель в {{ ydb-short-name }}. Источник — External Data Source (Microsoft SQL Server).

Подробнее про инструмент: [dbt](../../migration/dbt.md), [dbt-ydb](https://github.com/ydb-platform/dbt-ydb).

## Системные требования и подготовка {#prerequisites}

| Компонент | Требование |
| --- | --- |
| Кластер {{ ydb-short-name }} | Рабочий endpoint (например, `grpc://localhost:2136`, база `/local`) |
| Microsoft SQL Server | Сетевой или файловый доступ, учётная запись с правами `SELECT` |
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

### Проверка доступа к источнику (Microsoft SQL Server)

```bash
# sqlcmd или SSMS: SELECT 1
```

---

## Пошаговая инструкция {#steps}

Подробнее: [dbt](../../migration/dbt.md).

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

---

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/sales"
```
