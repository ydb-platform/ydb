# Перенос данных из IBM Db2 в {{ ydb-short-name }} с помощью Spark

Пошаговый рецепт: **IBM Db2** → {{ ydb-short-name }} через [Spark](../../tools/spark.md).

## Подготовка {#prerequisites}

{% include notitle [Spark](../../_includes/tools/spark-about.md) %}

{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (IBM Db2)

```bash
db2 "SELECT 1 FROM SYSIBM.SYSDUMMY1"
```

## Пошаговая инструкция {#steps}

Подробнее: [Spark](../../../../integrations/query-engines/spark.md).

```bash
spark-shell --master local[*] \
  --packages tech.ydb.spark:ydb-spark-connector-shaded:2.0.1 \
  --jars /path/to/db2jcc4.jar \
  --conf spark.executor.memory=4g
```

```scala
val df = spark.read.format("jdbc").options(Map(
  "url" -> "jdbc:db2://db2-host:50000/SAMPLE",
  "dbtable" -> "DB2INST1.CUSTOMERS",
  "user" -> "db2inst1",
  "password" -> "password",
  "driver" -> "com.ibm.db2.jcc.DB2Driver"
)).load()

df.write.format("ydb").options(Map(
  "url" -> "grpc://localhost:2136",
  "database" -> "/local",
  "table" -> "mydb/customers",
  "auth.mode" -> "NONE"
)).mode("append").save("mydb/customers")
```

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/customers"
```

Сравните с источником:

```bash
db2 "SELECT COUNT(*) FROM customers"
```
