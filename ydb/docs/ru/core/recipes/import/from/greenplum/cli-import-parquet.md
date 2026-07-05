# Перенос данных из Greenplum в {{ ydb-short-name }} через промежуточный Parquet с помощью YDB CLI

Пошаговый рецепт — перенос данных из **Greenplum** в {{ ydb-short-name }} через промежуточный Parquet, используя [YDB CLI](../../../../reference/ydb-cli/export-import/import-file.md).

Если промежуточный файл не нужен, рассмотрите прямой JDBC-импорт через [ydb-importer](ydb-importer.md).

## Подготовка {#prerequisites}

{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (Greenplum)

```bash
psql "postgresql://gpadmin:password@gp-master:5432/tpch" -c "SELECT 1"
```

## Пошаговая инструкция {#steps}

Подробнее о команде: [import file в YDB CLI](../../../../reference/ydb-cli/export-import/import-file.md).

### Шаг 1. Таблица в {{ ydb-short-name }}

```yql
CREATE TABLE `mydb/lineitem` (
    `l_orderkey` Int64,
    `l_linenumber` Int32,
    `l_quantity` Double,
    PRIMARY KEY (`l_orderkey`, `l_linenumber`)
);
```

### Шаг 2. Выгрузка из Greenplum в Parquet

Для распределённых таблиц удобно выгрузить Parquet через **Apache Spark** (JDBC к Greenplum) или **DuckDB** с `postgres_scanner` — протокол совместим с PostgreSQL.

Пример DuckDB:

```bash
duckdb :memory: -c "
  INSTALL postgres_scanner; LOAD postgres_scanner;
  COPY (
    SELECT l_orderkey, l_linenumber, l_quantity
    FROM postgres_scan('host=gp-master dbname=tpch user=gpadmin password=password', 'public', 'lineitem')
  ) TO '/tmp/lineitem.parquet' (FORMAT PARQUET);
"
```

Для очень больших таблиц используйте параллельный экспорт через `gpfdist` / внешние таблицы и Spark — см. [Spark](spark.md).

### Шаг 3. Импорт в {{ ydb-short-name }}

```bash
ydb -e grpc://localhost:2136 -d /local import file parquet \
  --path mydb/lineitem \
  /tmp/lineitem.parquet
```

При импорте Parquet учитывайте [маппинг типов Arrow/YQL](../../../../concepts/query_execution/federated_query/s3/arrow_types_mapping.md).

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/lineitem"
```

Сравните с источником:

```bash
psql "postgresql://gpadmin:password@gp-master:5432/tpch" -c "SELECT COUNT(*) FROM lineitem"
```
