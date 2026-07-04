# Перенос данных из IBM Db2 в {{ ydb-short-name }} с помощью Spark + ydb-spark-connector

Apache Spark читает таблицы IBM Db2 через JDBC и записывает в {{ ydb-short-name }} с помощью [YDB Spark Connector](../../query-engines/spark.md).

Подробнее про инструмент: [Spark](../../query-engines/spark.md), [ydb-spark-connector](https://github.com/ydb-platform/ydb-spark-connector).

## Системные требования и подготовка {#prerequisites}

| Компонент | Требование |
| --- | --- |
| Кластер {{ ydb-short-name }} | Рабочий endpoint (например, `grpc://localhost:2136`, база `/local`) |
| IBM Db2 | Сетевой или файловый доступ, учётная запись с правами `SELECT` |
| Кодировка | UTF-8 для текстовых данных |

### Установка {{ ydb-short-name }} CLI

```bash
curl -sSL https://install.ydb.tech/cli | bash
ydb version
```

Подробнее: [Установка YDB CLI](../../../reference/ydb-cli/install.md).

### Проверка подключения к {{ ydb-short-name }}

```bash
ydb -e grpc://localhost:2136 -d /local scheme ls
```

### Проверка доступа к источнику (IBM Db2)

```bash
db2 "SELECT 1 FROM SYSIBM.SYSDUMMY1"
```

---

## Пошаговая инструкция {#steps}

Подробнее: [Spark](../../query-engines/spark.md).

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

---

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/customers"
```

Сравните с источником:

```bash
db2 "SELECT COUNT(*) FROM customers"
```
