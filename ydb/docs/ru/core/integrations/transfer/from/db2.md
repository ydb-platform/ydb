# Перенос данных из IBM Db2 в {{ ydb-short-name }}

## Общая подготовка {#prerequisites}

| Компонент | Требование |
| --- | --- |
| {{ ydb-short-name }} | Рабочий endpoint |
| Db2 | TCP (50000), учётная запись с `SELECT` |
| JDBC | `db2jcc4.jar` |

---

## CLI import file {#cli-import-file}

Подробнее: [import file](../../reference/ydb-cli/export-import/import-file.md).

### Шаг 1. Таблица в {{ ydb-short-name }}

```yql
CREATE TABLE `mydb/customers` (
    `cust_id` Int64,
    `name` Text,
    PRIMARY KEY (`cust_id`)
);
```

### Шаг 2. Экспорт из Db2

```bash
db2 "CONNECT TO SAMPLE USER db2inst1 USING password"
db2 "EXPORT TO /tmp/customers.csv OF DEL MODIFIED BY NOCHARDEL COLDEL, SELECT cust_id, name FROM customers"
```

### Шаг 3. Импорт

```bash
ydb -e grpc://localhost:2136 -d /local import file csv \
  --path mydb/customers --header --delimiter "," /tmp/customers.csv
```

---

## Spark + ydb-spark-connector {#spark}

Подробнее: [Spark](../query-engines/spark.md).

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

## ydb-importer {#ydb-importer}

Подробнее: [импорт из JDBC](../data-migration/import-jdbc.md), [sample-db2.xml](https://github.com/ydb-platform/ydb-importer/blob/main/scripts/sample-db2.xml).

### Требования

* JDK 8+
* `db2jcc4.jar` в `lib/`

```xml
<source type="db2">
    <jdbc-class>com.ibm.db2.jcc.DB2Driver</jdbc-class>
    <jdbc-url>jdbc:db2://db2-host:50000/SAMPLE</jdbc-url>
    <username>db2inst1</username>
    <password>password</password>
</source>
<target type="ydb">
    <connection-string>grpc://localhost:2136?database=/local</connection-string>
    <auth-mode>NONE</auth-mode>
    <load-data>true</load-data>
</target>
```

```bash
./ydb-importer.sh db2-import.xml
```

---

## Проверка {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/customers"
db2 "SELECT COUNT(*) FROM customers"
```
