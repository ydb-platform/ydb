# Перенос данных из Oracle Database в {{ ydb-short-name }} с помощью Spark

Пошаговый рецепт: **Oracle Database** → {{ ydb-short-name }} через [Spark](../../../../integrations/query-engines/spark.md).

## Подготовка {#prerequisites}


{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (Oracle Database)

```bash
# SQL*Plus: SELECT 1 FROM DUAL;
```

## Пошаговая инструкция {#steps}

Подробнее: [Spark](../../../../integrations/query-engines/spark.md).

```bash
spark-shell --master local[*] \
  --packages tech.ydb.spark:ydb-spark-connector-shaded:2.0.1 \
  --jars /path/to/ojdbc11.jar \
  --conf spark.executor.memory=4g
```

```scala
val df = spark.read.format("jdbc").options(Map(
  "url" -> "jdbc:oracle:thin:@//ora-host:1521/ORCLPDB1",
  "dbtable" -> "HR.EMPLOYEES",
  "user" -> "hr",
  "password" -> "password",
  "driver" -> "oracle.jdbc.OracleDriver",
  "fetchsize" -> "10000"
)).load()

df.write.format("ydb").options(Map(
  "url" -> "grpc://localhost:2136",
  "database" -> "/local",
  "table" -> "mydb/employees",
  "auth.mode" -> "NONE"
)).mode("append").save("mydb/employees")
```

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM ora/HR/EMPLOYEES"
```

Сравните с источником:

```bash
# SQL*Plus: SELECT COUNT(*) FROM hr.employees;
```
