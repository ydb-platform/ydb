# Перенос данных из Greenplum в {{ ydb-short-name }} с помощью Spark

Пошаговый рецепт — перенос данных из **Greenplum** в {{ ydb-short-name }} с помощью [Spark](../../../../integrations/query-engines/spark.md).

## Подготовка {#prerequisites}


{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (Greenplum)

```bash
psql "postgresql://gpadmin:password@gp-master:5432/tpch" -c "SELECT 1"
```

## Пошаговая инструкция {#steps}

Подробнее: [Spark](../../../../integrations/query-engines/spark.md).

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

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/lineitem"
```

Сравните с источником:

```bash
psql "postgresql://gpadmin:password@gp-master:5432/tpch" -c "SELECT COUNT(*) FROM lineitem"
```
