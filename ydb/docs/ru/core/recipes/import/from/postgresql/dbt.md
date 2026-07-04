# Перенос данных из PostgreSQL в {{ ydb-short-name }} с помощью dbt

Пошаговый рецепт: **PostgreSQL** → {{ ydb-short-name }} через [dbt](../../../../integrations/migration/dbt.md).

## Подготовка {#prerequisites}


{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (PostgreSQL)

```bash
psql "postgresql://user:password@pg-host:5432/mydb" -c "SELECT 1"
```

## Пошаговая инструкция {#steps}

dbt материализует SQL-модели в {{ ydb-short-name }}. Источник — External Data Source; настройте его по [инструкции для федеративных запросов](federated-queries.md).

Подробнее: [интеграция dbt](../../../../integrations/migration/dbt.md), [репозиторий dbt-ydb](https://github.com/ydb-platform/dbt-ydb).

### Системные требования

* Python 3.10+
* dbt Core 1.8+ (не dbt Fusion 2.0)

### Шаг 1. Установите dbt-ydb

```bash
pip install dbt-ydb
```

### Шаг 2. Настройте профиль `~/.dbt/profiles.yml`

```yaml
ydb_pg_transfer:
  target: dev
  outputs:
    dev:
      type: ydb
      host: localhost
      port: 2136
      database: /local
      schema: mydb
```

### Шаг 3. Создайте модель `models/users.sql`

```sql
{{ config(materialized='table', primary_key='id') }}

SELECT id, name
FROM pg_src.users
```

Предварительно создайте External Data Source `pg_src` — см. [Федеративные запросы](federated-queries.md).

### Шаг 4. Запустите материализацию

```bash
dbt debug
dbt run --select users
```

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/users"
```

Сравните с источником:

```bash
psql "postgresql://user:password@pg-host:5432/mydb" -c "SELECT COUNT(*) FROM users"
```
