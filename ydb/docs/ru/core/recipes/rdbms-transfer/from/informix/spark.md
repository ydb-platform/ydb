# Перенос данных из IBM Informix в {{ ydb-short-name }} с помощью Spark

Пошаговый рецепт: **IBM Informix** → {{ ydb-short-name }} через [Spark](../../tools/spark.md).

## Подготовка {#prerequisites}

{% include notitle [Spark](../../_includes/tools/spark-about.md) %}

{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (IBM Informix)

```bash
# dbaccess: SELECT 1 FROM systables WHERE tabid=1;
```

## Пошаговая инструкция {#steps}

Подробнее: [Spark](../../../../integrations/query-engines/spark.md).

```bash
spark-shell --master local[*] \
  --packages tech.ydb.spark:ydb-spark-connector-shaded:2.0.1 \
  --jars /path/to/ifxjdbc.jar \
  --conf spark.executor.memory=4g
```

```scala
val df = spark.read.format("jdbc").options(Map(
  "url" -> "jdbc:informix-sqli://ifx-host:9088/stores_demo:INFORMIXSERVER=informix",
  "dbtable" -> "customer",
  "user" -> "informix",
  "password" -> "password",
  "driver" -> "com.informix.jdbc.IfxDriver"
)).load()

df.write.format("ydb").options(Map(
  "url" -> "grpc://localhost:2136",
  "database" -> "/local",
  "table" -> "mydb/customers",
  "auth.mode" -> "NONE"
)).mode("append").save("mydb/customers")
```

{% note info %}

Informix JDBC URL и имя `INFORMIXSERVER` зависят от вашей инсталляции — уточните в `sqlhosts` и `$INFORMIXSERVER`.

{% endnote %}

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/customers"
```

Сравните с источником:

```bash
# dbaccess: SELECT COUNT(*) FROM customer;
```
