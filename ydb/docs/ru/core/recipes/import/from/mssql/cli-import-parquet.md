# Перенос данных из Microsoft SQL Server в {{ ydb-short-name }} через промежуточный Parquet с помощью YDB CLI

Пошаговый рецепт — перенос данных из **Microsoft SQL Server** в {{ ydb-short-name }} через промежуточный Parquet, используя [YDB CLI](../../../../reference/ydb-cli/export-import/import-file.md).

Если промежуточный файл не нужен, рассмотрите прямой JDBC-импорт через [ydb-importer](ydb-importer.md).

## Подготовка {#prerequisites}

{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (Microsoft SQL Server)

```bash
# sqlcmd -S mssql-host -U user -P password -Q "SELECT 1"
```

## Пошаговая инструкция {#steps}

Подробнее о команде: [import file в YDB CLI](../../../../reference/ydb-cli/export-import/import-file.md).

### Шаг 1. Таблица в {{ ydb-short-name }}

```yql
CREATE TABLE `mydb/sales` (
    `id` Int64,
    `region` Text,
    PRIMARY KEY (`id`)
);
```

### Шаг 2. Экспорт из SQL Server в Parquet

SQL Server не экспортирует Parquet штатными средствами `bcp`. Рекомендуемый путь — **Apache Spark** (JDBC → Parquet); см. [Spark](spark.md).

Альтернатива — выгрузка через Azure Data Factory, SSIS или Python (`pyarrow` + `pyodbc`) в локальный файл `/tmp/sales.parquet`.

### Шаг 3. Импорт в {{ ydb-short-name }}

```bash
ydb -e grpc://localhost:2136 -d /local import file parquet \
  --path mydb/sales \
  /tmp/sales.parquet
```

При импорте Parquet учитывайте [маппинг типов Arrow/YQL](../../../../concepts/query_execution/federated_query/s3/arrow_types_mapping.md).

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/sales"
```
