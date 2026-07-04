# Перенос данных из Oracle Database в {{ ydb-short-name }}

Для Oracle доступны прямой JDBC-импорт (ydb-importer), а также универсальные пути через файлы и Spark.

## Общая подготовка {#prerequisites}

| Компонент | Требование |
| --- | --- |
| {{ ydb-short-name }} | Рабочий endpoint |
| Oracle | Listener (1521), учётная запись с `SELECT` на нужные таблицы |
| JDBC | Oracle JDBC driver (`ojdbc11.jar` или новее) |

---

## CLI import file {#cli-import-file}

Подробнее: [import file](../../reference/ydb-cli/export-import/import-file.md).

### Шаг 1. Таблица в {{ ydb-short-name }}

```yql
CREATE TABLE `mydb/employees` (
    `emp_id` Int64,
    `name` Text,
    PRIMARY KEY (`emp_id`)
);
```

### Шаг 2. Экспорт из Oracle

SQL*Plus / SQLcl:

```sql
SET MARKUP CSV ON DELIMITER ',' QUOTE ON
SET FEEDBACK OFF
SPOOL /tmp/employees.csv
SELECT emp_id, name FROM hr.employees;
SPOOL OFF
```

Или Data Pump / внешние таблицы Oracle → CSV/Parquet на общем хранилище.

### Шаг 3. Импорт

```bash
ydb -e grpc://localhost:2136 -d /local import file csv \
  --path mydb/employees --header /tmp/employees.csv
```

---

## Spark + ydb-spark-connector {#spark}

Подробнее: [Spark](../query-engines/spark.md).

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

## ydb-importer {#ydb-importer}

Основной рекомендуемый способ прямого переноса из Oracle.

Подробнее: [импорт из JDBC](../data-migration/import-jdbc.md), [sample-oracle.xml](https://github.com/ydb-platform/ydb-importer/blob/main/scripts/sample-oracle.xml).

### Требования

* JDK 8+
* [ydb-importer](https://github.com/ydb-platform/ydb-importer/releases)
* `ojdbc*.jar` в каталоге `lib/`

### Шаг 1. Установка

```bash
unzip ydb-importer-*.zip -d ~/ydb-importer
cp ojdbc11.jar ~/ydb-importer/lib/
```

### Шаг 2. Конфигурация `oracle-import.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<ydb-importer>
    <workers><pool size="4"/></workers>
    <source type="oracle">
        <jdbc-class>oracle.jdbc.driver.OracleDriver</jdbc-class>
        <jdbc-url>jdbc:oracle:thin:@//ora-host:1521/ORCLPDB1</jdbc-url>
        <username>hr</username>
        <password>password</password>
    </source>
    <target type="ydb">
        <connection-string>grpc://localhost:2136?database=/local</connection-string>
        <auth-mode>NONE</auth-mode>
        <load-data>true</load-data>
        <max-batch-rows>1000</max-batch-rows>
    </target>
    <table-options name="default">
        <table-name-format>ora/${schema}/${table}</table-name-format>
        <blob-name-format>ora/${schema}/${table}_${field}</blob-name-format>
    </table-options>
    <table-map options="default">
        <include-schemas regexp="true">HR</include-schemas>
    </table-map>
</ydb-importer>
```

### Шаг 3. Запуск

```bash
cd ~/ydb-importer
./ydb-importer.sh oracle-import.xml
```

{% note warning %}

Вложенные таблицы Oracle (nested tables) не поддерживаются. BLOB выносятся в отдельные таблицы — см. [ограничения ydb-importer](../data-migration/import-jdbc.md#blob).

{% endnote %}

---

## Проверка {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM ora/HR/EMPLOYEES"
```

Сравните с Oracle:

```sql
SELECT COUNT(*) FROM hr.employees;
```
