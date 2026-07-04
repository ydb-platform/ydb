# Перенос данных из ClickHouse в {{ ydb-short-name }}

## Общая подготовка {#prerequisites}

| Компонент | Требование |
| --- | --- |
| {{ ydb-short-name }} | Feature flag `enable_external_data_sources` для федеративных запросов |
| ClickHouse | HTTP(S)-интерфейс (порт 8123 или 8443) |

---

## CLI import file {#cli-import-file}

Подробнее: [import file](../../reference/ydb-cli/export-import/import-file.md).

### Шаг 1. Таблица в {{ ydb-short-name }}

```yql
CREATE TABLE `mydb/events` (
    `event_date` Date,
    `user_id` Int64,
    `value` Double,
    PRIMARY KEY (`event_date`, `user_id`)
);
```

### Шаг 2. Экспорт из ClickHouse

CSV:

```bash
clickhouse-client --host ch-host --query \
  "SELECT event_date, user_id, value FROM mydb.events FORMAT CSVWithNames" \
  > /tmp/events.csv
```

Parquet (рекомендуется для больших объёмов):

```bash
clickhouse-client --host ch-host --query \
  "SELECT event_date, user_id, value FROM mydb.events FORMAT Parquet" \
  > /tmp/events.parquet
```

### Шаг 3. Импорт в {{ ydb-short-name }}

```bash
# CSV
ydb -e grpc://localhost:2136 -d /local import file csv \
  --path mydb/events --header /tmp/events.csv

# Parquet
ydb -e grpc://localhost:2136 -d /local import file parquet \
  --path mydb/events /tmp/events.parquet
```

При импорте Parquet учитывайте [маппинг типов Arrow/YQL](../../concepts/query_execution/federated_query/s3/arrow_types_mapping.md).

---

## Spark + ydb-spark-connector {#spark}

Подробнее: [Spark](../query-engines/spark.md).

Чтение через JDBC ClickHouse:

```bash
spark-shell --master local[*] \
  --packages tech.ydb.spark:ydb-spark-connector-shaded:2.0.1,com.clickhouse:clickhouse-jdbc:0.6.3 \
  --conf spark.executor.memory=4g
```

```scala
val df = spark.read.format("jdbc").options(Map(
  "url" -> "jdbc:clickhouse://ch-host:8123/mydb",
  "dbtable" -> "events",
  "user" -> "default",
  "password" -> "",
  "driver" -> "com.clickhouse.jdbc.ClickHouseDriver"
)).load()

df.write.format("ydb").options(Map(
  "url" -> "grpc://localhost:2136",
  "database" -> "/local",
  "table" -> "mydb/events",
  "auth.mode" -> "NONE"
)).mode("append").save("mydb/events")
```

Альтернатива: прочитать Parquet через `spark.read.parquet` после экспорта из ClickHouse.

---

## Федеративные запросы {#federated-queries}

Подробнее: [ClickHouse](../../concepts/query_execution/federated_query/clickhouse.md), [импорт](../../concepts/query_execution/federated_query/import_and_export.md).

```yql
CREATE SECRET ch_password WITH (value = "secret");

CREATE EXTERNAL DATA SOURCE ch_src WITH (
    SOURCE_TYPE="ClickHouse",
    LOCATION="ch-host:8443",
    DATABASE_NAME="mydb",
    AUTH_METHOD="BASIC",
    LOGIN="default",
    PASSWORD_SECRET_PATH="ch_password",
    USE_TLS="TRUE"
);

CREATE TABLE `mydb/events` (
    `event_date` Date,
    `user_id` Int64,
    `value` Double,
    PRIMARY KEY (`event_date`, `user_id`)
);

UPSERT INTO `mydb/events`
SELECT * FROM ch_src.events;
```

---

## dbt (dbt-ydb) {#dbt}

Подробнее: [dbt](../migration/dbt.md).

После создания `ch_src`:

```sql
{{ config(materialized='table', primary_key='event_date, user_id', store_type='column') }}
SELECT event_date, user_id, value FROM ch_src.events
```

```bash
pip install dbt-ydb
dbt run --select events
```

---

## Проверка {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/events"
clickhouse-client --host ch-host --query "SELECT count() FROM mydb.events"
```
