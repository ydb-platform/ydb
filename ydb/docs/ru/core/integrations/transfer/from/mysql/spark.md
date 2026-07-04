# Перенос данных из MySQL / MariaDB в {{ ydb-short-name }} с помощью Spark + ydb-spark-connector

Apache Spark читает таблицы MySQL / MariaDB через JDBC и записывает в {{ ydb-short-name }} с помощью [YDB Spark Connector](../../query-engines/spark.md).

Подробнее про инструмент: [Spark](../../query-engines/spark.md), [ydb-spark-connector](https://github.com/ydb-platform/ydb-spark-connector).

## Системные требования и подготовка {#prerequisites}

| Компонент | Требование |
| --- | --- |
| Кластер {{ ydb-short-name }} | Рабочий endpoint (например, `grpc://localhost:2136`, база `/local`) |
| MySQL / MariaDB | Сетевой или файловый доступ, учётная запись с правами `SELECT` |
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

### Проверка доступа к источнику (MySQL / MariaDB)

```bash
mysql -h mysql-host -u user -p mydb -e "SELECT 1"
```

---

## Пошаговая инструкция {#steps}

Подробнее: [Spark](../../query-engines/spark.md).

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

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/orders"
```

Сравните с источником:

```bash
mysql -h mysql-host -u user -p mydb -e "SELECT COUNT(*) FROM orders"
```
