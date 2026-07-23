# Перенос данных из PostgreSQL в {{ ydb-short-name }} через промежуточный Parquet с помощью YDB CLI

Пошаговый рецепт — перенос данных из **PostgreSQL** в {{ ydb-short-name }} через промежуточный Parquet, используя [YDB CLI](../../../../reference/ydb-cli/export-import/import-file.md).

Если промежуточный файл не нужен, рассмотрите прямой JDBC-импорт через [ydb-importer](ydb-importer.md).

## Подготовка {#prerequisites}

{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (PostgreSQL)

```bash
psql "postgresql://user:password@pg-host:5432/mydb" -c "SELECT 1"
```

## Пошаговая инструкция {#steps}

Выгрузите данные из PostgreSQL в Parquet и загрузите в **уже созданную** таблицу {{ ydb-short-name }}.

Подробнее о команде: [import file в YDB CLI](../../../../reference/ydb-cli/export-import/import-file.md).

### Шаг 1. Создайте таблицу в {{ ydb-short-name }}

```yql
CREATE TABLE `mydb/users` (
    `id` Int64,
    `name` Text,
    PRIMARY KEY (`id`)
);
```

### Шаг 2. Выгрузите данные из PostgreSQL в Parquet

PostgreSQL не экспортирует Parquet нативно. Варианты:

**DuckDB** (если установлен):

```bash
duckdb :memory: -c "
  INSTALL postgres_scanner; LOAD postgres_scanner;
  COPY (
    SELECT id, name
    FROM postgres_scan('host=pg-host dbname=mydb user=user password=password', 'public', 'users')
  ) TO '/tmp/users.parquet' (FORMAT PARQUET);
"
```

**Apache Spark** — JDBC-чтение и запись Parquet; см. [Spark](spark.md).

### Шаг 3. Импортируйте Parquet в {{ ydb-short-name }}

```bash
ydb -e grpc://localhost:2136 -d /local import file parquet \
  --path mydb/users \
  /tmp/users.parquet
```

При импорте Parquet учитывайте [маппинг типов Arrow/YQL](../../../../concepts/query_execution/federated_query/s3/arrow_types_mapping.md).

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/users"
```

Сравните с источником:

```bash
psql "postgresql://user:password@pg-host:5432/mydb" -c "SELECT COUNT(*) FROM users"
```
