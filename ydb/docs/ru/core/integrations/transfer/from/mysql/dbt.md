# Перенос данных из MySQL / MariaDB в {{ ydb-short-name }} с помощью dbt (dbt-ydb)

dbt материализует SQL-модель в {{ ydb-short-name }}. Источник — External Data Source (MySQL / MariaDB).

Подробнее про инструмент: [dbt](../../migration/dbt.md), [dbt-ydb](https://github.com/ydb-platform/dbt-ydb).

## Системные требования и подготовка {#prerequisites}

| Компонент | Требование |
| --- | --- |
| Кластер {{ ydb-short-name }} | Рабочий endpoint (например, `grpc://localhost:2136`, база `/local`) |
| MySQL / MariaDB | Сетевой или файловый доступ, учётная запись с правами `SELECT` |
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

### Проверка доступа к источнику (MySQL / MariaDB)

```bash
mysql -h mysql-host -u user -p mydb -e "SELECT 1"
```

---

## Пошаговая инструкция {#steps}

Подробнее: [dbt](../../migration/dbt.md).

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

---

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/orders"
```

Сравните с источником:

```bash
mysql -h mysql-host -u user -p mydb -e "SELECT COUNT(*) FROM orders"
```
