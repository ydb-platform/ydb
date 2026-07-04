# Перенос данных из Greenplum в {{ ydb-short-name }}

Greenplum основан на PostgreSQL; многие шаги совпадают с [переносом из PostgreSQL](postgresql.md), но ниже — сценарии, типичные для Greenplum.

## Общая подготовка {#prerequisites}

| Компонент | Требование |
| --- | --- |
| {{ ydb-short-name }} | Feature flag `enable_external_data_sources` |
| Greenplum | Доступ к master-ноде, учётная запись с `SELECT` |

---

## CLI import file {#cli-import-file}

Подробнее: [import file](../../reference/ydb-cli/export-import/import-file.md), [импорт из PostgreSQL](../../postgresql/import.md) (Greenplum упомянут как сценарий файлового импорта).

### Шаг 1. Таблица в {{ ydb-short-name }}

```yql
CREATE TABLE `mydb/lineitem` (
    `l_orderkey` Int64,
    `l_linenumber` Int32,
    `l_quantity` Double,
    PRIMARY KEY (`l_orderkey`, `l_linenumber`)
);
```

### Шаг 2. Выгрузка из Greenplum

```bash
psql "postgresql://gpadmin:password@gp-master:5432/tpch" -c \
  "\copy (SELECT l_orderkey, l_linenumber, l_quantity FROM lineitem) TO '/tmp/lineitem.csv' WITH (FORMAT csv, HEADER true)"
```

Для распределённых таблиц выгрузка через master может быть медленной — используйте `gpfdist` / внешние таблицы Greenplum для параллельного экспорта в S3 или NFS, затем импортируйте файлы в {{ ydb-short-name }}.

### Шаг 3. Импорт

```bash
ydb -e grpc://localhost:2136 -d /local import file csv \
  --path mydb/lineitem --header /tmp/lineitem.csv
```

---

## Spark + ydb-spark-connector {#spark}

Подробнее: [Spark](../query-engines/spark.md).

JDBC через PostgreSQL-драйвер к master-ноде:

```bash
spark-shell --master local[*] \
  --packages tech.ydb.spark:ydb-spark-connector-shaded:2.0.1,org.postgresql:postgresql:42.7.3 \
  --conf spark.executor.memory=4g
```

```scala
val df = spark.read.format("jdbc").options(Map(
  "url" -> "jdbc:postgresql://gp-master:5432/tpch",
  "dbtable" -> "public.lineitem",
  "user" -> "gpadmin",
  "password" -> "password",
  "driver" -> "org.postgresql.Driver"
)).load()

df.write.format("ydb").options(Map(
  "url" -> "grpc://localhost:2136",
  "database" -> "/local",
  "table" -> "mydb/lineitem",
  "auth.mode" -> "NONE"
)).mode("append").save("mydb/lineitem")
```

---

## Федеративные запросы {#federated-queries}

Подробнее: [Greenplum](../../concepts/query_execution/federated_query/greenplum.md).

```yql
CREATE SECRET gp_password WITH (value = "secret");

CREATE EXTERNAL DATA SOURCE gp_src WITH (
    SOURCE_TYPE="Greenplum",
    LOCATION="gp-master:5432",
    DATABASE_NAME="tpch",
    AUTH_METHOD="BASIC",
    LOGIN="gpadmin",
    PASSWORD_SECRET_PATH="gp_password",
    PROTOCOL="NATIVE",
    SCHEMA="public"
);

CREATE TABLE `mydb/lineitem` (
    `l_orderkey` Int64,
    `l_linenumber` Int32,
    `l_quantity` Double,
    PRIMARY KEY (`l_orderkey`, `l_linenumber`)
);

UPSERT INTO `mydb/lineitem`
SELECT * FROM gp_src.lineitem;
```

---

## dbt (dbt-ydb) {#dbt}

Подробнее: [dbt](../migration/dbt.md).

```sql
{{ config(materialized='table', primary_key='l_orderkey, l_linenumber', store_type='column') }}
SELECT l_orderkey, l_linenumber, l_quantity FROM gp_src.lineitem
```

```bash
pip install dbt-ydb
dbt run --select lineitem
```

---

## Проверка {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/lineitem"
psql "postgresql://gpadmin:password@gp-master:5432/tpch" -c "SELECT COUNT(*) FROM lineitem"
```
