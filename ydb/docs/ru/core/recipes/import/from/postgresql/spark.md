# Перенос данных из PostgreSQL в {{ ydb-short-name }} с помощью Spark

Пошаговый рецепт — перенос данных из **PostgreSQL** в {{ ydb-short-name }} с помощью [Spark](../../../../integrations/query-engines/spark.md).

## Подготовка {#prerequisites}


{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Проверка доступа к источнику (PostgreSQL)

```bash
psql "postgresql://user:password@pg-host:5432/mydb" -c "SELECT 1"
```

## Пошаговая инструкция {#steps}

Подходит для больших объёмов и параллельного чтения через JDBC.

Подробнее: [{{ spark-name }}](../../../../integrations/query-engines/spark.md), [репозиторий ydb-spark-connector](https://github.com/ydb-platform/ydb-spark-connector).

### Системные требования

* Apache Spark 3.x
* JDK 11+
* JDBC-драйвер PostgreSQL (`org.postgresql:postgresql`)

### Шаг 1. Запустите Spark с коннекторами

```bash
spark-shell --master local[*] \
  --packages tech.ydb.spark:ydb-spark-connector-shaded:2.0.1,org.postgresql:postgresql:42.7.3 \
  --conf spark.executor.memory=4g
```

### Шаг 2. Прочитайте таблицу PostgreSQL и запишите в {{ ydb-short-name }}

{% list tabs %}

- Scala

  ```scala
  val pgUrl = "jdbc:postgresql://pg-host:5432/mydb"
  val pgOpts = Map(
    "url" -> pgUrl,
    "dbtable" -> "public.users",
    "user" -> "user",
    "password" -> "password",
    "driver" -> "org.postgresql.Driver"
  )
  val df = spark.read.format("jdbc").options(pgOpts).load()

  val ydbOpts = Map(
    "url" -> "grpc://localhost:2136",
    "database" -> "/local",
    "table" -> "mydb/users",
    "auth.mode" -> "NONE"
  )
  df.write.format("ydb").options(ydbOpts).mode("append").save("mydb/users")
  ```

- Python

  ```python
  pg_df = spark.read.format("jdbc").options(
      url="jdbc:postgresql://pg-host:5432/mydb",
      dbtable="public.users",
      user="user",
      password="password",
      driver="org.postgresql.Driver",
  ).load()

  pg_df.write.format("ydb").options(
      url="grpc://localhost:2136",
      database="/local",
      table="mydb/users",
      **{"auth.mode": "NONE"},
  ).mode("append").save("mydb/users")
  ```

{% endlist %}

{% note info %}

При отсутствии целевой таблицы Spark Connector может создать её автоматически — см. [опции автосоздания](../../../../integrations/query-engines/spark.md).

{% endnote %}

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/users"
```

Сравните с источником:

```bash
psql "postgresql://user:password@pg-host:5432/mydb" -c "SELECT COUNT(*) FROM users"
```
