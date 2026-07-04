# Перенос данных из Microsoft SQL Server в {{ ydb-short-name }}

## Общая подготовка {#prerequisites}

| Компонент | Требование |
| --- | --- |
| {{ ydb-short-name }} | Рабочий endpoint |
| SQL Server | TCP-доступ (порт 1433 по умолчанию), учётная запись с `SELECT` |
| TLS | При `encrypt=true` — доверенный сертификат или `trustServerCertificate=true` в JDBC |

```bash
ydb -e grpc://localhost:2136 -d /local scheme ls
```

---

## CLI import file {#cli-import-file}

Подробнее: [import file](../../reference/ydb-cli/export-import/import-file.md).

### Шаг 1. Таблица в {{ ydb-short-name }}

```yql
CREATE TABLE `mydb/sales` (
    `id` Int64,
    `region` Text,
    PRIMARY KEY (`id`)
);
```

### Шаг 2. Экспорт из SQL Server

Через `bcp`:

```bash
bcp "SELECT id, region FROM mydb.dbo.sales" queryout /tmp/sales.csv \
  -S mssql-host -U user -P password -c -t,
```

Или через SSMS / Azure Data Studio: Export as CSV (UTF-8).

### Шаг 3. Импорт

```bash
ydb -e grpc://localhost:2136 -d /local import file csv \
  --path mydb/sales --header --delimiter "," /tmp/sales.csv
```

---

## Spark + ydb-spark-connector {#spark}

Подробнее: [Spark](../query-engines/spark.md).

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

---

## Федеративные запросы {#federated-queries}

Подробнее: [Microsoft SQL Server](../../concepts/query_execution/federated_query/ms_sql_server.md).

```yql
CREATE SECRET mssql_password WITH (value = "secret");

CREATE EXTERNAL DATA SOURCE mssql_src WITH (
    SOURCE_TYPE="MsSQLServer",
    LOCATION="mssql-host:1433",
    DATABASE_NAME="mydb",
    AUTH_METHOD="BASIC",
    LOGIN="user",
    PASSWORD_SECRET_PATH="mssql_password",
    USE_TLS="TRUE"
);

CREATE TABLE `mydb/sales` (
    `id` Int64,
    `region` Text,
    PRIMARY KEY (`id`)
);

UPSERT INTO `mydb/sales`
SELECT * FROM mssql_src.sales;
```

---

## dbt (dbt-ydb) {#dbt}

Подробнее: [dbt](../migration/dbt.md).

```bash
pip install dbt-ydb
```

`models/sales.sql`:

```sql
{{ config(materialized='table', primary_key='id') }}
SELECT id, region FROM mssql_src.sales
```

```bash
dbt run --select sales
```

---

## ydb-importer {#ydb-importer}

Подробнее: [импорт из JDBC](../data-migration/import-jdbc.md), [sample-mssql.xml](https://github.com/ydb-platform/ydb-importer/blob/main/scripts/sample-mssql.xml).

### Требования

* JDK 8+
* `mssql-jdbc-*.jar` в `lib/` дистрибутива ydb-importer

```xml
<source type="mssql">
    <jdbc-class>com.microsoft.sqlserver.jdbc.SQLServerDriver</jdbc-class>
    <jdbc-url>jdbc:sqlserver://mssql-host:1433;databaseName=mydb;encrypt=true;trustServerCertificate=true</jdbc-url>
    <username>user</username>
    <password>password</password>
</source>
<target type="ydb">
    <connection-string>grpc://localhost:2136?database=/local</connection-string>
    <auth-mode>NONE</auth-mode>
    <load-data>true</load-data>
</target>
```

```bash
./ydb-importer.sh mssql-import.xml
```

{% note info %}

Пространственные типы SQL Server не поддерживаются ydb-importer.

{% endnote %}

---

## Проверка {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/sales"
```
