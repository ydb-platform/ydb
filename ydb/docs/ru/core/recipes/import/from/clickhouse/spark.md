# Перенос данных из ClickHouse в {{ ydb-short-name }} с помощью Spark

Пошаговый рецепт: **ClickHouse** → {{ ydb-short-name }} через [Spark](../../../../integrations/query-engines/spark.md).

## Подготовка {#prerequisites}


{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (ClickHouse)

```bash
clickhouse-client --host ch-host --query "SELECT 1"
```

## Пошаговая инструкция {#steps}

Подробнее: [Spark](../../../../integrations/query-engines/spark.md).

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

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/events"
```

Сравните с источником:

```bash
clickhouse-client --host ch-host --query "SELECT count() FROM mydb.events"
```
