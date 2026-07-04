# Перенос данных из Greenplum в {{ ydb-short-name }} с помощью dbt (dbt-ydb)

dbt материализует SQL-модель в {{ ydb-short-name }}. Источник — External Data Source (Greenplum).

Подробнее про инструмент: [dbt](../../migration/dbt.md), [dbt-ydb](https://github.com/ydb-platform/dbt-ydb).

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

Подробнее: [dbt](../../migration/dbt.md).

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

---

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/lineitem"
```

Сравните с источником:

```bash
psql "postgresql://gpadmin:password@gp-master:5432/tpch" -c "SELECT COUNT(*) FROM lineitem"
```
