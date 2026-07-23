# Импорт Parquet в {{ ydb-short-name }} с помощью Spark

Пошаговый рецепт — загрузка данных из **Parquet-файла** в {{ ydb-short-name }} с помощью [Spark](../../../../integrations/query-engines/spark.md).

## Подготовка {#prerequisites}

{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

Parquet-файл должен быть доступен драйверам и executor'ам Spark (локальный путь, HDFS, S3 и т. п.).

## Пошаговая инструкция {#steps}

Подробнее: [Spark](../../../../integrations/query-engines/spark.md).

```bash
spark-shell --master local[*] \
  --packages tech.ydb.spark:ydb-spark-connector-shaded:2.0.1 \
  --conf spark.executor.memory=4g
```

Чтение Parquet и запись в {{ ydb-short-name }}:

```scala
val df = spark.read.parquet("/path/to/events.parquet")

df.write.format("ydb").options(Map(
  "url" -> "grpc://localhost:2136",
  "database" -> "/local",
  "table" -> "mydb/events",
  "auth.mode" -> "NONE"
)).mode("append").save("mydb/events")
```

Python:

```python
df = spark.read.parquet("/path/to/events.parquet")

df.write.format("ydb").options(
    url="grpc://localhost:2136",
    database="/local",
    table="mydb/events",
    **{"auth.mode": "NONE"},
).mode("append").save("mydb/events")
```

Целевая таблица должна существовать в {{ ydb-short-name }} до записи.

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/events"
```
