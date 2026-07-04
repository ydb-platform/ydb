# Перенос данных из SQLite в {{ ydb-short-name }} с помощью Spark

Пошаговый рецепт: **SQLite** → {{ ydb-short-name }} через [Spark](../../tools/spark.md).

## Подготовка {#prerequisites}

{% include notitle [Spark](../../_includes/tools/spark-about.md) %}

{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (SQLite)

```bash
sqlite3 /path/to/app.db "SELECT 1"
```

## Пошаговая инструкция {#steps}

Подробнее: [Spark](../../../../integrations/query-engines/spark.md).

```bash
spark-shell --master local[*] \
  --packages tech.ydb.spark:ydb-spark-connector-shaded:2.0.1,org.xerial:sqlite-jdbc:3.46.0.0 \
  --conf spark.executor.memory=4g
```

```scala
val df = spark.read.format("jdbc").options(Map(
  "url" -> "jdbc:sqlite:/path/to/app.db",
  "dbtable" -> "items",
  "driver" -> "org.sqlite.JDBC"
)).load()

df.write.format("ydb").options(Map(
  "url" -> "grpc://localhost:2136",
  "database" -> "/local",
  "table" -> "mydb/items",
  "auth.mode" -> "NONE"
)).mode("append").save("mydb/items")
```

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/items"
```

Сравните с источником:

```bash
sqlite3 /path/to/app.db "SELECT COUNT(*) FROM items;"
```
