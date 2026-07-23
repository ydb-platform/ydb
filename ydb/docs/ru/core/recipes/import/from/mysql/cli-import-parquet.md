# Перенос данных из MySQL / MariaDB в {{ ydb-short-name }} через промежуточный Parquet с помощью YDB CLI

Пошаговый рецепт — перенос данных из **MySQL / MariaDB** в {{ ydb-short-name }} через промежуточный Parquet, используя [YDB CLI](../../../../reference/ydb-cli/export-import/import-file.md).

Если промежуточный файл не нужен, рассмотрите прямой JDBC-импорт через [ydb-importer](ydb-importer.md) или [mysql2ydb](mysql2ydb.md).

## Подготовка {#prerequisites}

{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (MySQL / MariaDB)

```bash
mysql -h mysql-host -u user -p mydb -e "SELECT 1"
```

## Пошаговая инструкция {#steps}

Подробнее о команде: [import file в YDB CLI](../../../../reference/ydb-cli/export-import/import-file.md).

### Шаг 1. Создайте таблицу в {{ ydb-short-name }}

```yql
CREATE TABLE `mydb/orders` (
    `id` Int64,
    `amount` Double,
    PRIMARY KEY (`id`)
);
```

### Шаг 2. Выгрузите данные в Parquet

MySQL не экспортирует Parquet нативно. Варианты:

**DuckDB** (если установлен):

```bash
duckdb :memory: -c "
  INSTALL mysql; LOAD mysql;
  ATTACH 'host=mysql-host user=user password=password port=3306 database=mydb' AS mysqldb (TYPE MYSQL);
  COPY (SELECT id, amount FROM mysqldb.orders) TO '/tmp/orders.parquet' (FORMAT PARQUET);
"
```

**Apache Spark** — JDBC-чтение и запись Parquet; см. [Spark](spark.md).

### Шаг 3. Импортируйте Parquet в {{ ydb-short-name }}

```bash
ydb -e grpc://localhost:2136 -d /local import file parquet \
  --path mydb/orders \
  /tmp/orders.parquet
```

При импорте Parquet учитывайте [маппинг типов Arrow/YQL](../../../../concepts/query_execution/federated_query/s3/arrow_types_mapping.md).

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/orders"
```

Сравните с источником:

```bash
mysql -h mysql-host -u user -p mydb -e "SELECT COUNT(*) FROM orders"
```
