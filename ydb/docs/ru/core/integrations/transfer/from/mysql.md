# Перенос данных из MySQL / MariaDB в {{ ydb-short-name }}

MySQL и MariaDB используют совместимый wire protocol и JDBC-драйверы; ниже инструкции применимы к обеим СУБД.

## Общая подготовка {#prerequisites}

| Компонент | Требование |
| --- | --- |
| Кластер {{ ydb-short-name }} | Рабочий endpoint |
| MySQL / MariaDB | Сетевой доступ, учётная запись с правами `SELECT` |
| Кодировка | UTF-8 |

```bash
# YDB CLI
curl -sSL https://install.ydb.tech/cli | bash
ydb -e grpc://localhost:2136 -d /local scheme ls

# MySQL
mysql -h mysql-host -u user -p mydb -e "SELECT 1"
```

---

## CLI import file {#cli-import-file}

Подробнее: [import file](../../reference/ydb-cli/export-import/import-file.md).

### Шаг 1. Создайте таблицу в {{ ydb-short-name }}

```yql
CREATE TABLE `mydb/orders` (
    `id` Int64,
    `amount` Double,
    PRIMARY KEY (`id`)
);
```

### Шаг 2. Выгрузите данные в CSV

```bash
mysql -h mysql-host -u user -p mydb \
  -e "SELECT id, amount FROM orders" \
  --batch --raw --skip-column-names \
  | sed 's/\t/,/g' > /tmp/orders.csv
```

Для надёжного CSV с заголовком используйте `INTO OUTFILE` (если разрешено на сервере) или клиентские утилиты (`mydumper`, DBeaver export).

### Шаг 3. Импортируйте в {{ ydb-short-name }}

```bash
ydb -e grpc://localhost:2136 -d /local import file csv \
  --path mydb/orders --header --columns id,amount /tmp/orders.csv
```

---

## Spark + ydb-spark-connector {#spark}

Подробнее: [Spark](../query-engines/spark.md).

```bash
spark-shell --master local[*] \
  --packages tech.ydb.spark:ydb-spark-connector-shaded:2.0.1,com.mysql:mysql-connector-j:9.0.0 \
  --conf spark.executor.memory=4g
```

{% list tabs %}

- Scala

  ```scala
  val df = spark.read.format("jdbc").options(Map(
    "url" -> "jdbc:mysql://mysql-host:3306/mydb",
    "dbtable" -> "orders",
    "user" -> "user",
    "password" -> "password",
    "driver" -> "com.mysql.cj.jdbc.Driver"
  )).load()

  df.write.format("ydb").options(Map(
    "url" -> "grpc://localhost:2136",
    "database" -> "/local",
    "table" -> "mydb/orders",
    "auth.mode" -> "NONE"
  )).mode("append").save("mydb/orders")
  ```

- Python

  ```python
  df = spark.read.format("jdbc").options(
      url="jdbc:mysql://mysql-host:3306/mydb",
      dbtable="orders",
      user="user",
      password="password",
      driver="com.mysql.cj.jdbc.Driver",
  ).load()

  df.write.format("ydb").options(
      url="grpc://localhost:2136",
      database="/local",
      table="mydb/orders",
      **{"auth.mode": "NONE"},
  ).mode("append").save("mydb/orders")
  ```

{% endlist %}

Для MariaDB замените URL на `jdbc:mariadb://…` и драйвер `org.mariadb.jdbc.Driver`.

---

## Федеративные запросы {#federated-queries}

Подробнее: [MySQL как внешний источник](../../concepts/query_execution/federated_query/mysql.md), [импорт](../../concepts/query_execution/federated_query/import_and_export.md).

```yql
CREATE SECRET mysql_password WITH (value = "secret");

CREATE EXTERNAL DATA SOURCE mysql_src WITH (
    SOURCE_TYPE="MySQL",
    LOCATION="mysql-host:3306",
    DATABASE_NAME="mydb",
    AUTH_METHOD="BASIC",
    LOGIN="user",
    PASSWORD_SECRET_PATH="mysql_password"
);

CREATE TABLE `mydb/orders` (
    `id` Int64,
    `amount` Double,
    PRIMARY KEY (`id`)
);

UPSERT INTO `mydb/orders`
SELECT * FROM mysql_src.orders;
```

---

## dbt (dbt-ydb) {#dbt}

Подробнее: [dbt](../migration/dbt.md).

```bash
pip install dbt-ydb
```

Модель `models/orders.sql` после создания `mysql_src`:

```sql
{{ config(materialized='table', primary_key='id') }}
SELECT id, amount FROM mysql_src.orders
```

```bash
dbt run --select orders
```

---

## ydb-importer {#ydb-importer}

Подробнее: [импорт из JDBC](../data-migration/import-jdbc.md), [sample-mysql.xml](https://github.com/ydb-platform/ydb-importer/blob/main/scripts/sample-mysql.xml).

### Требования

* JDK 8+
* [ydb-importer](https://github.com/ydb-platform/ydb-importer/releases)
* `mysql-connector-j-*.jar` или `mariadb-java-client-*.jar` в `lib/`

```bash
unzip ydb-importer-*.zip -d ~/ydb-importer
cp mysql-connector-j-*.jar ~/ydb-importer/lib/
```

Минимальный фрагмент конфигурации:

```xml
<source type="mysql">
    <jdbc-class>com.mysql.cj.jdbc.Driver</jdbc-class>
    <jdbc-url>jdbc:mysql://mysql-host:3306/mydb</jdbc-url>
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
./ydb-importer.sh mysql-import.xml
```

---

## mysql2ydb {#mysql2ydb}

Специализированный инструмент для копии MySQL «один к одному», с возобновлением и поддержкой больших таблиц.

Подробнее: [импорт из MySQL](../data-migration/import-mysql.md), [репозиторий mysql-ydb-importer](https://github.com/ydb-platform/mysql-ydb-importer).

### Требования

* Go (версия из `go.mod` репозитория)
* Файл `~/.my.cnf` или флаг `-mysql`

### Шаг 1. Сборка

```bash
git clone https://github.com/ydb-platform/mysql-ydb-importer.git
cd mysql-ydb-importer
go build -o mysql2ydb ./cmd/mysql2ydb
```

### Шаг 2. Настройте `~/.my.cnf`

```ini
[client]
user = myuser
password = mypass
host = mysql-host
port = 3306
database = mydb
```

### Шаг 3. Запуск

Только данные (схема уже в {{ ydb-short-name }}):

```bash
./mysql2ydb -ydb "grpc://localhost:2136" -data-only -batch-size 10000
```

Схема и данные:

```bash
./mysql2ydb -mysql "user:pass@tcp(mysql-host:3306)/mydb" \
  -ydb "grpc://localhost:2136" -batch-size 10000
```

{% note warning %}

У каждой таблицы MySQL должен быть `PRIMARY KEY` (или подготовьте схему вручную и используйте `-data-only`).

{% endnote %}

---

## Проверка {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/orders"
mysql -h mysql-host -u user -p mydb -e "SELECT COUNT(*) FROM orders"
```
