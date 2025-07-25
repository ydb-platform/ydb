# {{ spark-name }}

{{ spark-name }} — is a fast, open-source cluster computing system for big data processing that works with various data stores and supports multiple programming languages (Scala, Java, Python, R). {{ spark-name }} can work with {{ ydb-full-name }} using [Spark Connector](https://github.com/ydb-platform/ydb-spark-connector), a special module that provides implementations of core  {{ spark-name }} primitives.

## How to use {#usage}

To work with {{ ydb-short-name }} in {{ spark-name }} you need to add the {{ ydb-short-name }} Spark Connector to your Apache Spark™ [driver](https://spark.apache.org/docs/latest/cluster-overview.html). That can be done in several ways:

* Download the connector dependency directly from Maven Central using the `--packages` option. It's recommended to use the latest published [version](https://mvnrepository.com/artifact/tech.ydb.spark/ydb-spark-connector)

  ```shell
  # Run spark-shell
  ~ $ spark-shell --master <master-url> --packages tech.ydb.spark:ydb-spark-connector:2.0.1
  # Or spark-sql
  ~ $ spark-sql --master <master-url> --packages tech.ydb.spark:ydb-spark-connector:2.0.1
  ```

* Download the latest version of the shaded connector (a connector build that includes all dependencies) from [GitHub](https://github.com/ydb-platform/ydb-spark-connector/releases) or [Maven Central](https://mvnrepository.com/artifact/tech.ydb.spark/ydb-spark-connector-shaded) and specify the downloaded artifact in the `--jars` option

  ```shell
  # Run spark-shell
  ~ $ spark-shell --master <master-url> --jars ~/Download/ydb-spark-connector-shaded-2.0.1.jar
  # Or spark-sql
  ~ $ spark-sql --master <master-url> --jars ~/Download/ydb-spark-connector-shaded-2.0.1.jar
  ```

* You can also copy the downloaded shaded artifact to the `jars`  folder of your {{ spark-name }} distribution. In this case, no additional options need to be specified

### Use DataFrame API {#dataframe-api}

The DataFrame API allows to work with {{ ydb-short-name }} in interactive `spark-shell` or `pyspark`, as well as when writing code with `Java`, `Scala`, or `Python` for `spark-submit`.

Create a `DataFrame` referencing a {{ ydb-short-name }} table

```scala
val ydb_df = spark.read.format("ydb").options(<options>).load(<table-path>)
```

Write a `DataFrame` to a {{ ydb-short-name }} table

```scala
any_dataframe.write.format("ydb").options(<options>).mode("append").load(<table-path>)
```

{% note info %}

For writing data to {{ ydb-short-name }} it's recommended to use the `append` mode, which uses [batch data loading](../../dev/batch-upload.md)

{% endnote %}

A more detailed example is provided in [Spark-shell example](#example-spark-shell)

### Use Catalog API {#catalog-api}

Catalogs allow you to work with {{ ydb-short-name }} in interactive `spark-sql` or execute SQL queries via the `spark.sql` method.
In this case, to access {{ ydb-short-name }}, you need to create an {{ spark-name }} catalog by specifying the following properties (it's allowed to define multiple catalogs with different names for access to different {{ ydb-short-name }} databases):

```properties
# Mandatory catalog's driver name
spark.sql.catalog.<catalog_name>=tech.ydb.spark.connector.YdbCatalog
# Mandatory option, the url of database
spark.sql.catalog.<catalog_name>.url=grpc://my-ydb-host:2135/cluster/database
# Other options are not mandatory and may be specified by upon request
spark.sql.catalog.<catalog_name>.auth.token.file=/home/username/.token
```

After that, you can work with  {{ ydb-short-name }} tables through standard {{ spark-name }} SQL queries.
Note that you should use a dot `.` as a separator in the table path.

```sql
SELECT * FROM my_ydb.stackoverflow.posts LIMIT;
```

A more detailed example is provided in [Spark-sql example](#example-spark-sql)

## {{ ydb-short-name }} Spark Connector options {#options}

The behavior of the {{ ydb-short-name }} Spark Connector is configured using options that can be passed as a `Map` using the `options` method, or specified one by one using the `option` method. Each `DataFrame` and even each individual operation on a `DataFrame` can have its own configuration of options.

### Опции подключения {#auth-options}

* `url` — a required parameter with the {{ ydb-short-name }} connection string. Has the form `grpc[s]://<endpoint>:<port>/<database>[?<options>]`
   Examples:
   - Local Docker container with anonymous authentication and without TLS:<br/>`grpc://localhost:2136/local`
   - Remote self-hosted cluster:<br/>`grpcs://my-private-cluster:2135/Root/my-database?secureConnectionCertificate=~/myCertificate.cer`
   - Cloud database instance with a token:<br/>`grpcs://ydb.my-cloud.com:2135/my_folder/test_database?tokenFile=~/my_token`
   - Cloud database instance with a service account key:<br/>`grpcs://ydb.my-cloud.com:2135/my_folder/test_database?saKeyFile=~/sa_key.json`

* `auth.use_env` —  if set to `true`, authentication based on environment variables will be used
* `auth.use_metadata` —  if set to `true`, metadata based authentication mode will be used. Can be specified directly in the url as the `useMetadata` option
* `auth.login` и `auth.password` — login and password for static authentication
* `auth.token` — authentication using the specified token
* `auth.token.file` — authentication using a token from the specified file. Can be specified directly in the url as the `tokenFile` option
* `auth.ca.text` — specifies the certificate value for establishing a TLS connection
* `auth.ca.file` — specifies the path to the certificate for establishing a TLS connection. Can be specified directly in the `url` as the `secureConnectionCertificate` option
* `auth.sakey.text` — uses to specify the key content for authentication using a service account key
* `auth.sakey.file` — uses to specify the path to the key file for authentication using a service account key. Can be specified directly in the `url` as the `saKeyFile` option

### Table autocreation options {#autocreate-options}

* `table.autocreate` — аutomatically create a table if it doesn't exist. Enabled by default
* `table.type` — the type of automatically created table. Possible values are `rows` for creating a row-oriented table (default) and `columns` for creating a column-oriented table
* `table.primary_keys` — a comma-separated list of columns to use as the primary key. If this option is not provided, a new column with random content will be used for the key
* `table.auto_pk_name` — the name of the column for the randomly created key. Default value is `_spark_key`

## Spark-shell example {#example-spark-shell}

As an example, we'll show how to load a list of all StackOverflow posts from 2020 year into {{ ydb-short-name }}. This data can be downloaded by link [https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/2020.parquet](https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/2020.parquet)

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

Let's display the schema of the parquet file and count the number of rows it contains

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

Then add a new column with the year to this DataSet and store it all to a columnar {{ ydb-short-name }} table

```shell
scala> val my_ydb = Map("url" -> "grpcs://ydb.my-host.net:2135/preprod/spark-test?tokenFile=~/.token")
my_ydb: scala.collection.immutable.Map[String,String] = Map(url -> grpcs://ydb.my-host.net:2135/preprod/spark-test?tokenFile=~/.token)

scala> so_posts2020.withColumn("Year", lit(2020)).write.format("ydb").options(my_ydb).option("table.type", "column").option("table.primary_keys", "Id").mode("append").save("stackoverflow/posts");
```

As a result, we can read the stored data from {{ ydb-short-name }} table and, for example, count the number of posts that have an accepted answer

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

## Spark-sql example {#example-spark-sql}

As an example, we'll show how to load a list of all StackOverflow posts from 2020 year into {{ ydb-short-name }}. This data can be downloaded by link [https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/2020.parquet](https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/2020.parquet)

First, let's run `spark-sql` with the configured `my_ydb` catalog

```shell
~ $ spark-sql --master <master-url> --packages tech.ydb.spark:ydb-spark-connector:2.0.1 \
     --conf spark.sql.catalog.my_ydb=tech.ydb.spark.connector.YdbCatalog
     --conf spark.sql.catalog.my_ydb.url=grpcs://ydb.my-host.net:2135/preprod/spark-test?tokenFile=~/.token
spark-sql (default)>
```

Let's validate the current state of the connected database and confirm the absence of the `stackoverflow/posts` table

```shell
spark-sql (default)> SHOW NAMESPACES FROM my_ydb;
stackoverflow
Time taken: 0.11 seconds, Fetched 1 row(s)
spark-sql (default)> SHOW TABLES FROM my_ydb.stackoverflow;
Time taken: 0.041 seconds
```

Let's count the number of rows in the original parquet file

```shell
spark-sql (default)> SELECT COUNT(*) FROM parquet.`/home/username/2020.parquet`;
4304021
```

Let's add a new column with the year and copy it all to a new {{ ydb-short-name }} table

```shell
spark-sql (default)> CREATE TABLE my_ydb.stackoverflow.posts AS SELECT *, 2020 as Year FROM parquet.`/home/username/2020.parquet`;
Time taken: 85.225 seconds
```

Let's verify that the new table has appeared in the {{ ydb-short-name }}  database

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

As a result, we can read the stored data from {{ ydb-short-name }} table and, for example, count the number of posts that have an accepted answer

```shell
spark-sql (default)> SELECT COUNT(*) FROM my_ydb.stackoverflow.posts;
4304021
Time taken: 19.726 seconds, Fetched 1 row(s)
spark-sql (default)> SELECT COUNT(*) FROM my_ydb.stackoverflow.posts WHERE AcceptedAnswerId > 0;
843780
Time taken: 6.599 seconds, Fetched 1 row(s)
```