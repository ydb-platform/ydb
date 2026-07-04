# Перенос данных из IBM Informix в {{ ydb-short-name }}

Для Informix в {{ ydb-short-name }} нет федеративного коннектора и отдельного JDBC-импортёра в обзорной матрице; используйте выгрузку в файл или Spark с JDBC.

## Общая подготовка {#prerequisites}

| Компонент | Требование |
| --- | --- |
| {{ ydb-short-name }} | Рабочий endpoint |
| Informix | SQLI listener (9088), учётная запись с `SELECT` |
| JDBC | `ifxjdbc.jar` (Informix JDBC driver) |

---

## CLI import file {#cli-import-file}

Подробнее: [import file](../../reference/ydb-cli/export-import/import-file.md).

### Шаг 1. Таблица в {{ ydb-short-name }}

```yql
CREATE TABLE `mydb/customers` (
    `customer_num` Int64,
    `fname` Text,
    PRIMARY KEY (`customer_num`)
);
```

### Шаг 2. Экспорт из Informix

Через `dbexport` / `UNLOAD TO`:

```sql
UNLOAD TO '/tmp/customers.unl' DELIMITER ','
SELECT customer_num, fname FROM customer;
```

Преобразуйте в CSV с заголовком или укажите колонки явно при импорте.

### Шаг 3. Импорт

```bash
ydb -e grpc://localhost:2136 -d /local import file csv \
  --path mydb/customers \
  --columns customer_num,fname \
  --delimiter "," \
  /tmp/customers.unl
```

---

## Spark + ydb-spark-connector {#spark}

Подробнее: [Spark](../query-engines/spark.md).

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

---

## Проверка {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/customers"
```

В Informix:

```sql
SELECT COUNT(*) FROM customer;
```
