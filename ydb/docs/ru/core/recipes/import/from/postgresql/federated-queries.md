# Перенос данных из PostgreSQL в {{ ydb-short-name }} с помощью Федеративные запросы

Пошаговый рецепт: **PostgreSQL** → {{ ydb-short-name }} через [Федеративные запросы](../../../../concepts/query_execution/federated_query/import_and_export.md).

## Подготовка {#prerequisites}


{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (PostgreSQL)

```bash
psql "postgresql://user:password@pg-host:5432/mydb" -c "SELECT 1"
```

## Пошаговая инструкция {#steps}

Данные читаются с PostgreSQL напрямую узлами {{ ydb-short-name }} и записываются в локальную таблицу одним YQL-запросом.

Подробнее: [импорт через федеративные запросы](../../../../concepts/query_execution/federated_query/import_and_export.md), [PostgreSQL как внешний источник](../../../../concepts/query_execution/federated_query/postgresql.md).

{% note warning %}

Федеративные коннекторы к PostgreSQL находятся в стадии Preview. На кластере должен быть включён feature flag `enable_external_data_sources`.

{% endnote %}

### Шаг 1. Создайте секрет с паролем

```yql
CREATE SECRET pg_password WITH (value = "secret");
```

### Шаг 2. Создайте External Data Source

```yql
CREATE EXTERNAL DATA SOURCE pg_src WITH (
    SOURCE_TYPE="PostgreSQL",
    LOCATION="pg-host:5432",
    DATABASE_NAME="mydb",
    AUTH_METHOD="BASIC",
    LOGIN="user",
    PASSWORD_SECRET_PATH="pg_password",
    PROTOCOL="NATIVE",
    SCHEMA="public"
);
```

### Шаг 3. Создайте целевую таблицу и импортируйте данные

```yql
CREATE TABLE `mydb/users` (
    `id` Int64,
    `name` Text,
    PRIMARY KEY (`id`)
);

UPSERT INTO `mydb/users`
SELECT * FROM pg_src.users;
```

Для колоночных таблиц `UPSERT` выполняется параллельно.

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/users"
```

Сравните с источником:

```bash
psql "postgresql://user:password@pg-host:5432/mydb" -c "SELECT COUNT(*) FROM users"
```
