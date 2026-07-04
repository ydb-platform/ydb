# Перенос данных из Microsoft SQL Server в {{ ydb-short-name }} с помощью Spark

Пошаговый рецепт: **Microsoft SQL Server** → {{ ydb-short-name }} через [Spark](../../tools/spark.md).

## Подготовка {#prerequisites}

{% include notitle [Spark](../../_includes/tools/spark-about.md) %}

{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (Microsoft SQL Server)

```bash
# sqlcmd -S mssql-host -U user -P password -Q "SELECT 1"
```

## Пошаговая инструкция {#steps}

Подробнее: [Spark](../../../../integrations/query-engines/spark.md).

```bash
spark-shell --master local[*] \
  --packages tech.ydb.spark:ydb-spark-connector-shaded:2.0.1,com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11 \
  --conf spark.executor.memory=4g
```

```scala
val df = spark.read.format("jdbc").options(Map(
  "url" -> "jdbc:sqlserver://mssql-host:1433;databaseName=mydb;encrypt=true;trustServerCertificate=true",
  "dbtable" -> "dbo.sales",
  "user" -> "user",
  "password" -> "password",
  "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver"
)).load()

df.write.format("ydb").options(Map(
  "url" -> "grpc://localhost:2136",
  "database" -> "/local",
  "table" -> "mydb/sales",
  "auth.mode" -> "NONE"
)).mode("append").save("mydb/sales")
```

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/sales"
```
