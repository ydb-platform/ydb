# {{ spark-name }}

{{ spark-name }} — это быстрая система кластерных вычислений с открытым исходным кодом для обработки больших данных, позволяющая работать с различными хранилищами данных, и поддерживающая несколько языков программирования (Scala, Java, Python, R). {{ spark-name }} может работать с {{ ydb-full-name }} с помощью [Spark Connector](https://github.com/ydb-platform/ydb-spark-connector), специального модуля, предоставляющего реализацию основных примитивов {{ spark-name }}.

## Где взять и как использовать {#usage}

Для использования {{ ydb-short-name }} в {{ spark-name }} необходимо добавить {{ ydb-short-name }} Spark Connector в [драйвер {{ spark-name }}](https://spark.apache.org/docs/latest/cluster-overview.html). Это можно сделать несколькими способами:

* Загрузить коннектор напрямую из Maven Central с помощью опции `--packages`. Рекомендуется использовать последнюю опубликованную [версию](https://mvnrepository.com/artifact/tech.ydb.spark/ydb-spark-connector):

  ```shell
  # Запустить spark-shell
  ~ $ spark-shell --master <master-url> --packages tech.ydb.spark:ydb-spark-connector:2.0.1
  # Или spark-sql
  ~ $ spark-sql --master <master-url> --packages tech.ydb.spark:ydb-spark-connector:2.0.1
  ```

* Скачать последнюю версию shaded-сборки (вариант коннектора, включающий в себя все зависимости) из [GitHub](https://github.com/ydb-platform/ydb-spark-connector/releases) или [Маven Central](https://mvnrepository.com/artifact/tech.ydb.spark/ydb-spark-connector-shaded) и указать скачанный артефакт в опции `--jars`:

  ```shell
  # Запустить spark-shell
  ~ $ spark-shell --master <master-url> --jars ~/Download/ydb-spark-connector-shaded-2.0.1.jar
  # Или spark-sql
  ~ $ spark-sql --master <master-url> --jars ~/Download/ydb-spark-connector-shaded-2.0.1.jar
  ```

* Так же скачанную shaded-сборку можно скопировать в папку `jars` дистрибутива {{ spark-name }}. В таком случае никаких опций указывать не требуется.

### Работа через DataFrame API {#dataframe-api}

DataFrame API позволяет работать с {{ ydb-short-name }} в интерактивных `spark-shell` или `pyspark`, а так же при написании кода на `Java`, `Scala` или `Python` для `spark-submit`.

Создать `DataFrame`, ссылающийся на таблицу {{ ydb-short-name }}:

```scala
val ydb_df = spark.read.format("ydb").options(<options>).load(<table-path>)
```

Записать `DataFrame` в таблицу {{ ydb-short-name }}:

```scala
any_dataframe.write.format("ydb").options(<options>).mode("append").load(<table-path>)
```

{% note info %}

При записи данных в {{ ydb-short-name }} рекомендуется использовать режим `append`, который использует [пакетную загрузку данных](../../dev/batch-upload.md).

{% endnote %}

Более подробный пример приведен в разделе [Пример работы с {{ ydb-short-name }} в spark-shell](#example-spark-shell).

### Работа через Catalog API {#catalog-api}

С помощью каталогов возможна работа с {{ ydb-short-name }} в интерактивном режиме `spark-sql` или при выполнении SQL-запросов через метод `spark.sql`.
В этом случае для доступа к {{ ydb-short-name }} нужно создать каталог {{ spark-name }}, указав следующие опции (можно определить несколько каталогов с разными именами для разных баз данных {{ ydb-short-name }}):

```properties
# Обязательный параметр типа каталога
spark.sql.catalog.<catalog_name>=tech.ydb.spark.connector.YdbCatalog
# Обязательный параметр url
spark.sql.catalog.<catalog_name>.url=grpc://my-ydb-host:2135/cluster/database
# Все остальные параметры не обязательны и указываются только при необходимости
spark.sql.catalog.<catalog_name>.auth.token.file=/home/username/.token
```

После задания каталогов можно работать в таблицами {{ ydb-short-name }} через стандартные SQL запросы {{ spark-name }}. Обратите внимание, что в качестве разделителя в пути к таблице нужно использовать `.`:

```sql
SELECT * FROM my_ydb.stackoverflow.posts LIMIT;
```

Более подробный пример приведен в разделе [Пример работы с {{ ydb-short-name }} в spark-sql](#example-spark-sql).

## Список параметров {{ ydb-short-name }} Spark Connector {#options}

Поведение {{ ydb-short-name }} Spark Connector настраивается с помощью опций, которые как могут передаваться в виде одного набора с помощью метода `options`, так и указываться по одной с помощью метода `option`. При этом каждый `DataFrame` и даже каждая отдельная операция над `DataFrame` может иметь свой набор опций.

### Опции подключения {#auth-options}

* `url` — обязательный параметр с адресом подключения к {{ ydb-short-name }}. Имеет вид `grpc[s]://<endpoint>:<port>/<database>[?<options>]`
   Примеры использования:
   - Локальный Docker контейнер с анонимной аутентификацией и без TLS:<br/>`grpc://localhost:2136/local`
   - Удаленный кластер, размещенный на собственном сервере:<br/>`grpcs://my-private-cluster:2135/Root/my-database?secureConnectionCertificate=~/myCertificate.cer`
   - Экземпляр облачной базы данных с токеном:<br/>`grpcs://ydb.my-cloud.com:2135/my_folder/test_database?tokenFile=~/my_token`
   - Экземпляр облачной базы данных с файлом сервисного аккаунта:<br/>`grpcs://ydb.my-cloud.com:2135/my_folder/test_database?saKeyFile=~/sa_key.json`

* `auth.use_env` — если указано `true`, то будет использоваться режим аутентификации на основе переменных среды окружения;
* `auth.use_metadata` — если указано `true`, то будет использоваться режим аутентификации Metadata. Может быть указан прямо в `url` в виде опции `useMetadata`;
* `auth.login` и `auth.password` — логин и пароль для статической аутентификации;
* `auth.token` — аутентификация с использованием указанного токена;
* `auth.token.file` — аутентификация с использованием токена из указанного файла. Может быть указан прямо в `url` в виде опции `tokenFile`;
* `auth.ca.text` — указывает значение сертификата для установки TLS-соединения;
* `auth.ca.file` — указывает путь к сертификату для установки TLS-соединения. Может быть указан прямо в `url` в виде опции `secureConnectionCertificate`;
* `auth.sakey.text` — можно указать содержимое ключа для аутентификации по ключу сервисного аккаунта;
* `auth.sakey.file` — можно указать путь к файлу ключа для аутентификации по ключу сервисного аккаунта. Может быть указан прямо в `url` в виде опции `saKeyFile`.

### Опции автоматического создания таблиц {#autocreate-options}

* `table.autocreate` — автоматически создавать таблицу при ее отсутствии. По умолчанию включено;
* `table.type` — тип автоматически создаваемой таблицы. Возможные варианты:
    - `rows` — для создания строчной таблицы (по умолчанию);
    - `columns` — для создания колоночной таблицы;
* `table.primary_keys` — разделённый запятой список колонок для использования в качестве первичного ключа. При отсутствии этой опции для ключа будет использоваться новая колонка со случайным содержимым;
* `table.auto_pk_name` — имя колонки для случайного создаваемого ключа. По умолчанию `_spark_key`.

## Пример работы с {{ ydb-short-name }} в spark-shell {#example-spark-shell}

В качестве примера покажем как загрузить в {{ ydb-short-name }} список всех постов StackOverflow за 2020 год. Эти данные доступны в виде parquet-файла, расположенного по адресу [https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/2020.parquet](https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/2020.parquet):

```shell
~ $ spark-shell --master <master-url> --packages tech.ydb.spark:ydb-spark-connector:2.0.1
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.5.4
      /_/

Using Scala version 2.12.18 (OpenJDK 64-Bit Server VM, Java 17.0.15)
Type in expressions to have them evaluated.
Type :help for more information.
```

Выведем схему файла с данными и посмотрим число строк в нём:

```shell
scala> val so_posts2020 = spark.read.format("parquet").load("/home/username/2020.parquet")
so_posts2020: org.apache.spark.sql.DataFrame = [Id: bigint, PostTypeId: bigint ... 20 more fields]

scala> so_posts2020.printSchema
root
 |-- Id: long (nullable = true)
 |-- PostTypeId: long (nullable = true)
 |-- AcceptedAnswerId: long (nullable = true)
 |-- CreationDate: timestamp (nullable = true)
 |-- Score: long (nullable = true)
 |-- ViewCount: long (nullable = true)
 |-- Body: binary (nullable = true)
 |-- OwnerUserId: long (nullable = true)
 |-- OwnerDisplayName: binary (nullable = true)
 |-- LastEditorUserId: long (nullable = true)
 |-- LastEditorDisplayName: binary (nullable = true)
 |-- LastEditDate: timestamp (nullable = true)
 |-- LastActivityDate: timestamp (nullable = true)
 |-- Title: binary (nullable = true)
 |-- Tags: binary (nullable = true)
 |-- AnswerCount: long (nullable = true)
 |-- CommentCount: long (nullable = true)
 |-- FavoriteCount: long (nullable = true)
 |-- ContentLicense: binary (nullable = true)
 |-- ParentId: binary (nullable = true)
 |-- CommunityOwnedDate: timestamp (nullable = true)
 |-- ClosedDate: timestamp (nullable = true)

scala> so_posts2020.count
res1: Long = 4304021
```

Добавим к данному DataSet новую колонку с годом и запишем всё это в колоночную таблицу {{ ydb-short-name }}:

```shell
scala> val my_ydb = Map("url" -> "grpcs://ydb.my-host.net:2135/preprod/spark-test?tokenFile=~/.token")
my_ydb: scala.collection.immutable.Map[String,String] = Map(url -> grpcs://ydb.my-host.net:2135/preprod/spark-test?tokenFile=~/.token)

scala> so_posts2020.withColumn("Year", lit(2020)).write.format("ydb").options(my_ydb).option("table.type", "column").option("table.primary_keys", "Id").mode("append").save("stackoverflow/posts");
```

В итоге мы можем прочитать записанные данные из {{ ydb-short-name }} и, например, подсчитать число постов, у которых есть подтверждённый ответ:

```shell
scala> val ydb_posts2020 = spark.read.format("ydb").options(my_ydb).load("stackoverflow/posts")
ydb_posts2020: org.apache.spark.sql.DataFrame = [Id: bigint, PostTypeId: bigint ... 21 more fields]

scala> ydb_posts2020.printSchema
root
 |-- Id: long (nullable = false)
 |-- PostTypeId: long (nullable = true)
 |-- AcceptedAnswerId: long (nullable = true)
 |-- CreationDate: timestamp (nullable = true)
 |-- Score: long (nullable = true)
 |-- ViewCount: long (nullable = true)
 |-- Body: binary (nullable = true)
 |-- OwnerUserId: long (nullable = true)
 |-- OwnerDisplayName: binary (nullable = true)
 |-- LastEditorUserId: long (nullable = true)
 |-- LastEditorDisplayName: binary (nullable = true)
 |-- LastEditDate: timestamp (nullable = true)
 |-- LastActivityDate: timestamp (nullable = true)
 |-- Title: binary (nullable = true)
 |-- Tags: binary (nullable = true)
 |-- AnswerCount: long (nullable = true)
 |-- CommentCount: long (nullable = true)
 |-- FavoriteCount: long (nullable = true)
 |-- ContentLicense: binary (nullable = true)
 |-- ParentId: binary (nullable = true)
 |-- CommunityOwnedDate: timestamp (nullable = true)
 |-- ClosedDate: timestamp (nullable = true)
 |-- Year: integer (nullable = true)

scala> ydb_posts2020.count
res3: Long = 4304021

scala> ydb_posts2020.filter(col("AcceptedAnswerId") > 0).count
res4: Long = 843780
```

## Пример работы с {{ ydb-short-name }} в spark-sql {#example-spark-sql}

В качестве примера покажем как загрузить в {{ ydb-short-name }} список всех постов StackOverflow за 2020 год. Эти данные доступны в виде parquet-файла, расположенного по адресу [https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/2020.parquet](https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/2020.parquet):

Для начала запустим `spark-sql` c настроенным каталогом `my_ydb`:

```shell
~ $ spark-sql --master <master-url> --packages tech.ydb.spark:ydb-spark-connector:2.0.1 \
     --conf spark.sql.catalog.my_ydb=tech.ydb.spark.connector.YdbCatalog
     --conf spark.sql.catalog.my_ydb.url=grpcs://ydb.my-host.net:2135/preprod/spark-test?tokenFile=~/.token
spark-sql (default)>
```

Просмотрим текущее содержимое подключенной базы данных и убедимся в отсутствии таблицы `stackoverflow/posts`:

```shell
spark-sql (default)> SHOW NAMESPACES FROM my_ydb;
stackoverflow
Time taken: 0.11 seconds, Fetched 1 row(s)
spark-sql (default)> SHOW TABLES FROM my_ydb.stackoverflow;
Time taken: 0.041 seconds
```

Подсчитаем число строк в оригинальном файле:

```shell
spark-sql (default)> SELECT COUNT(*) FROM parquet.`/home/username/2020.parquet`;
4304021
```

Добавим новую колонку с годом и запишем всё это в таблицу {{ ydb-short-name }}:

```shell
spark-sql (default)> CREATE TABLE my_ydb.stackoverflow.posts AS SELECT *, 2020 as Year FROM parquet.`/home/username/2020.parquet`;
Time taken: 85.225 seconds
```

Убедимся, что новая таблица появилась в базе данных {{ ydb-short-name }}:

```shell
spark-sql (default)> SHOW TABLES FROM my_ydb.stackoverflow;
posts
Time taken: 0.07 seconds, Fetched 1 row(s)
spark-sql (default)> DESCRIBE TABLE my_ydb.stackoverflow.posts;
Id                    bigint
PostTypeId            bigint
AcceptedAnswerId      bigint
CreationDate          timestamp
Score                 bigint
ViewCount             bigint
Body                  binary
OwnerUserId           bigint
OwnerDisplayName      binary
LastEditorUserId      bigint
LastEditorDisplayName binary
LastEditDate          timestamp
LastActivityDate      timestamp
Title                 binary
Tags                  binary
AnswerCount           bigint
CommentCount          bigint
FavoriteCount         bigint
ContentLicense        binary
ParentId              binary
CommunityOwnedDate    timestamp
ClosedDate            timestamp
Year                  int
_spark_key            string
```

В итоге мы можем прочитать записанные данные из {{ ydb-short-name }} и, например, подсчитать число постов, у которых есть подтверждённый ответ:

```shell
spark-sql (default)> SELECT COUNT(*) FROM my_ydb.stackoverflow.posts;
4304021
Time taken: 19.726 seconds, Fetched 1 row(s)
spark-sql (default)> SELECT COUNT(*) FROM my_ydb.stackoverflow.posts WHERE AcceptedAnswerId > 0;
843780
Time taken: 6.599 seconds, Fetched 1 row(s)
```