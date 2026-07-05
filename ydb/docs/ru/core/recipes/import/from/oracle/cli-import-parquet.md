# Перенос данных из Oracle Database в {{ ydb-short-name }} через промежуточный Parquet с помощью YDB CLI

Пошаговый рецепт — перенос данных из **Oracle Database** в {{ ydb-short-name }} через промежуточный Parquet, используя [YDB CLI](../../../../reference/ydb-cli/export-import/import-file.md).

Если промежуточный файл не нужен, рассмотрите прямой JDBC-импорт через [ydb-importer](ydb-importer.md).

## Подготовка {#prerequisites}

{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (Oracle Database)

```bash
# SQL*Plus: SELECT 1 FROM DUAL;
```

## Пошаговая инструкция {#steps}

Подробнее о команде: [import file в YDB CLI](../../../../reference/ydb-cli/export-import/import-file.md).

### Шаг 1. Таблица в {{ ydb-short-name }}

```yql
CREATE TABLE `mydb/employees` (
    `emp_id` Int64,
    `name` Text,
    PRIMARY KEY (`emp_id`)
);
```

### Шаг 2. Экспорт из Oracle в Parquet

Oracle не экспортирует Parquet одной штатной командой. Варианты:

* **Apache Spark** — JDBC-чтение и запись Parquet; см. [Spark](spark.md).
* **Внешние таблицы / Data Pump** — выгрузка в Parquet на общее хранилище (S3, NFS) средствами вашего ETL-конвейера.

Полученный файл, например `/tmp/employees.parquet`, импортируйте в {{ ydb-short-name }}.

### Шаг 3. Импорт в {{ ydb-short-name }}

```bash
ydb -e grpc://localhost:2136 -d /local import file parquet \
  --path mydb/employees \
  /tmp/employees.parquet
```

При импорте Parquet учитывайте [маппинг типов Arrow/YQL](../../../../concepts/query_execution/federated_query/s3/arrow_types_mapping.md).

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/employees"
```

Сравните с источником:

```bash
# SQL*Plus: SELECT COUNT(*) FROM hr.employees;
```
