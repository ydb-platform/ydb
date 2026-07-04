# Перенос данных из Oracle Database в {{ ydb-short-name }} с помощью Spark + ydb-spark-connector

Apache Spark читает таблицы Oracle Database через JDBC и записывает в {{ ydb-short-name }} с помощью [YDB Spark Connector](../../query-engines/spark.md).

Подробнее про инструмент: [Spark](../../query-engines/spark.md), [ydb-spark-connector](https://github.com/ydb-platform/ydb-spark-connector).

## Системные требования и подготовка {#prerequisites}

| Компонент | Требование |
| --- | --- |
| Кластер {{ ydb-short-name }} | Рабочий endpoint (например, `grpc://localhost:2136`, база `/local`) |
| Oracle Database | Сетевой или файловый доступ, учётная запись с правами `SELECT` |
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

### Проверка доступа к источнику (Oracle Database)

```bash
# SQL*Plus: SELECT 1 FROM DUAL;
```

---

## Пошаговая инструкция {#steps}

Подробнее: [Spark](../../query-engines/spark.md).

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

---

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM ora/HR/EMPLOYEES"
```

Сравните с источником:

```bash
SELECT COUNT(*) FROM hr.employees;
```
