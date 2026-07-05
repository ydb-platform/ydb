# Перенос данных из MySQL / MariaDB в {{ ydb-short-name }} с помощью Spark

Пошаговый рецепт — перенос данных из **MySQL / MariaDB** в {{ ydb-short-name }} с помощью [Spark](../../../../integrations/query-engines/spark.md).

## Подготовка {#prerequisites}


{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (MySQL / MariaDB)

```bash
mysql -h mysql-host -u user -p mydb -e "SELECT 1"
```

## Пошаговая инструкция {#steps}

Подробнее: [Spark](../../../../integrations/query-engines/spark.md).

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

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/orders"
```

Сравните с источником:

```bash
mysql -h mysql-host -u user -p mydb -e "SELECT COUNT(*) FROM orders"
```
