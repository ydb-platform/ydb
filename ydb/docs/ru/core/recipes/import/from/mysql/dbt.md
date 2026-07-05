# Перенос данных из MySQL / MariaDB в {{ ydb-short-name }} с помощью dbt

Пошаговый рецепт — перенос данных из **MySQL / MariaDB** в {{ ydb-short-name }} с помощью [dbt](../../../../integrations/migration/dbt.md).

## Подготовка {#prerequisites}


{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (MySQL / MariaDB)

```bash
mysql -h mysql-host -u user -p mydb -e "SELECT 1"
```

## Пошаговая инструкция {#steps}

Подробнее: [dbt](../../../../integrations/migration/dbt.md).

### Шаг 1. Создайте External Data Source

См. [Федеративные запросы](federated-queries.md) (источник `mysql_src`).

### Шаг 2. Установите dbt-ydb и создайте модель

```bash
pip install dbt-ydb
```

Модель `models/orders.sql`:

```sql
{{ config(materialized='table', primary_key='id') }}
SELECT id, amount FROM mysql_src.orders
```

```bash
dbt run --select orders
```

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/orders"
```

Сравните с источником:

```bash
mysql -h mysql-host -u user -p mydb -e "SELECT COUNT(*) FROM orders"
```
