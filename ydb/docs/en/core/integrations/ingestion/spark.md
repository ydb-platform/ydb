# {{ spark-name }}

{{ spark-name }} is an open-source, distributed processing system used for big data workloads. It utilizes in-memory caching and optimized query execution for fast analytic queries against data of any size. It provides development APIs in Java, Scala, Python, and R, and supports code reuse across multiple workloads—batch processing, interactive queries, real-time analytics, machine learning, and graph processing. {{ spark-name }} can work with {{ ydb-full-name }} using the [{{ ydb-full-name }} Spark Connector](https://github.com/ydb-platform/ydb-spark-connector), a special module that implements core {{ spark-name }} primitives. It supports:

* Distribution of operations across {{ ydb-full-name }} table partitions
* Scalable {{ ydb-full-name }} table readings and writing
* Automatic creation of tables if they do not exist

{% note info %}

The connector may require additional memory on the {{ spark-name }} executor side to work with better speed and performance. 4 GB or more memory per [executor](https://spark.apache.org/docs/latest/configuration.html#application-properties) is highly recommended.

{% endnote %}

## How to Use {#usage}

To work with {{ ydb-short-name }} in {{ spark-name }}, you need to add the {{ ydb-short-name }} Spark Connector to your {{ spark-name }} [driver](https://spark.apache.org/docs/latest/cluster-overview.html). This can be done in several ways:

* Download the connector dependency directly from Maven Central using the `--packages` option. It's recommended to use the latest published [version](https://central.sonatype.com/artifact/tech.ydb.spark/ydb-spark-connector):

  {% list tabs %}

  - Spark Shell

    ```shell
    ~ $ spark-shell --master <master-url> --packages tech.ydb.spark:ydb-spark-connector-shaded:2.0.1 --conf spark.executor.memory=4g
    ```

  - PySpark

    ```shell
    ~ $ pyspark --master <master-url> --packages tech.ydb.spark:ydb-spark-connector-shaded:2.0.1 --conf spark.executor.memory=4g
    ```

  - Spark SQL

    ```shell
    ~ $ spark-sql --master <master-url> --packages tech.ydb.spark:ydb-spark-connector-shaded:2.0.1 --conf spark.executor.memory=4g
    ```

  {% endlist %}

* Download the latest version of the shaded connector (a connector build that includes all dependencies) from [GitHub](https://github.com/ydb-platform/ydb-spark-connector/releases) or [Maven Central](https://central.sonatype.com/artifact/tech.ydb.spark/ydb-spark-connector-shaded) and specify the downloaded artifact in the `--jars` option:

  {% list tabs %}

  - Spark Shell

    ```shell
    ~ $ spark-shell --master <master-url> --jars ~/Download/ydb-spark-connector-shaded-2.0.1.jar --conf spark.executor.memory=4g
    ```

  - PySpark

    ```shell
    ~ $ pyspark --master <master-url> --jars ~/Download/ydb-spark-connector-shaded-2.0.1.jar  --conf spark.executor.memory=4g
    ```

  - Spark SQL

    ```shell
    ~ $ spark-sql --master <master-url> --jars ~/Download/ydb-spark-connector-shaded-2.0.1.jar  --conf spark.executor.memory=4g
    ```

  {% endlist %}

* You can also copy the downloaded shaded artifact to the `jars` folder of your {{ spark-name }} distribution. In this case, no additional options need to be specified.

### Use DataFrame API {#dataframe-api}

[The DataFrame API](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html) allows you to work with {{ ydb-short-name }} in an interactive `spark-shell` or `pyspark` session, as well as when writing code in `Java`, `Scala`, or `Python` for `spark-submit`.

To create a `DataFrame`, you need to specify the `ydb` format, pass a set of [connection options](#connection-options) and the path to the {{ ydb-short-name }} table:

{% list tabs %}

- Scala

  ```scala
  val ydb_df = spark.read.format("ydb").options(<options>).load("<table-path>")
  ```

- Python

  ```python
  ydb_df = spark.read.format("ydb").options(<options>).load("<table-path>")
  ```

{% endlist %}

To save any `DataFrame` in the table {{ ydb-short-name }}, you similarly need to specify the `ydb` format, [connection options](#connection-options) and the path to the table:

{% list tabs %}

- Scala

  ```scala
  any_dataframe.write.format("ydb").options(<options>).mode("append").save("<table-path>")
  ```

- Python

  ```python
  any_dataframe.write.format("ydb").options(<options>).mode("append").save("<table-path>")
  ```

{% endlist %}

{% note info %}

For writing data to {{ ydb-short-name }} it is recommended to use the `append` mode, which uses [batch data loading](../../dev/batch-upload.md). If the table specified in the `save()` method does not exist, it will be created automatically according to [the table autocreation options](#autocreate-options).

{% endnote %}

A more detailed example is provided in the [Spark-shell example](#example-spark-shell).

### Use Catalog API {#catalog-api}

Catalogs allow you to work with {{ ydb-short-name }} in interactive `spark-sql` sessions or execute SQL queries via the `spark.sql` method.
To access {{ ydb-short-name }}, you need to add a catalog by specifying the following [{{ spark-name }} properties](https://spark.apache.org/docs/latest/configuration.html#spark-properties). You can define multiple catalogs with different names to access to different {{ ydb-short-name }} databases:

```properties
# Mandatory catalog's driver name
spark.sql.catalog.<catalog_name>=tech.ydb.spark.connector.YdbCatalog
# Mandatory option, the url of database
spark.sql.catalog.<catalog_name>.url=<ydb-connection-url>
# Other options are not mandatory and may be specified as necessary
spark.sql.catalog.<catalog_name>.<param-name>=<param-value>
```

After that, you can work with  {{ ydb-short-name }} tables through standard {{ spark-name }} SQL queries.
Note that you should use a dot `.` as a separator in the table path.

```sql
SELECT * FROM <catalog_name>.<table-path> LIMIT 10;
```

A more detailed example is provided in the [Spark SQL example](#example-spark-sql).

## {{ ydb-short-name }} Spark Connector Options {#options}

The behavior of the {{ ydb-short-name }} Spark Connector is configured using options that can be passed as one set with the `options` method or specified individually with the `option` method. Each `DataFrame` and each individual operation on a `DataFrame` can have its own configuration of options.

### Connection Options {#connection-options}

* `url` — a required parameter with the {{ ydb-short-name }} connection string in the following format:
    `grpc[s]://<endpoint>:<port>/<database>[?<options>]`
    Examples:
    - Local Docker container with anonymous authentication and without TLS:<br/>`grpc://localhost:2136/local`
    - Remote self-hosted cluster:<br/>`grpcs://my-private-cluster:2135/Root/my-database?secureConnectionCertificate=~/myCertificate.cer`
    - Cloud database instance with a token:<br/>`grpcs://ydb.my-cloud.com:2135/my_folder/test_database?tokenFile=~/my_token`
    - Cloud database instance with a service account key:<br/>`grpcs://ydb.my-cloud.com:2135/my_folder/test_database?saKeyFile=~/sa_key.json`

* `auth.use_env` —  if set to `true`, authentication based on [environment variables](../../reference/ydb-sdk/auth.md#env) will be used.
* `auth.use_metadata` —  if set to `true`, [metadata-based](../../security/authentication.md#iam) authentication mode will be used. You can specify it directly in `url` as the `useMetadata` option.
* `auth.login` and `auth.password` — login and password for [static authentication](../../security/authentication.md#static-credentials).
* `auth.token` — authentication using the specified [Access Token](../../security/authentication.md#iam).
* `auth.token.file` — authentication using [Access Token](../../security/authentication.md#iam) from the specified file. You can specify it directly in `url` as the `tokenFile` option.
* `auth.ca.text` — specifies the [certificate](../../concepts/connect.md#tls-cert) value for establishing a TLS connection.
* `auth.ca.file` — specifies the path to the [certificate](../../concepts/connect.md#tls-cert) for establishing a TLS connection. You can specify it directly in `url` as the `secureConnectionCertificate` option.
* `auth.sakey.text` — used to specify the key content for authentication with [a service account key](../../security/authentication.md#iam).
* `auth.sakey.file` — used to specify the path to the key file for authentication with [a service account key](../../security/authentication.md#iam). You can specify it directly in `url` as the `saKeyFile` option.

### Table Autocreation Options {#autocreate-options}

{% note tip %}

If you need the table to have some custom settings configured, create it manually beforehand with [CREATE TABLE](../../yql/reference/syntax/create_table/index.md) or modify it afterward with [ALTER TABLE](../../yql/reference/syntax/alter_table/index.md).

{% endnote %}

* `table.autocreate` — if set to `true`, then when writing to a non-existent table, it will be created automatically. Enabled by default;
* `table.type` — the type of automatically created table. Possible values:
    - `row` for creating a [row-oriented table](../../concepts/glossary.md#row-oriented-table) (default).
    - `column` for creating a [column-oriented table](../../concepts/glossary.md#column-oriented-table).
* `table.primary_keys` — a comma-separated list of columns to use as the primary key. If this option is not provided, a new column with random content will be used for the key.
* `table.auto_pk_name` — the name of the column for the randomly created key. This column will be created with the type `Utf8` and will be filled with random [UUID v4](https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_4_(random)) values. Default value is `_spark_key`.

## Spark Shell and PySpark Example {#example-spark-shell}

As an example, we'll show how to load a list of all Stack Overflow posts from 2020 into {{ ydb-short-name }}. This data can be downloaded from the following link: [https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/2020.parquet](https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/2020.parquet)

{% list tabs %}

- Spark Shell

  ```shell
  ~ $ spark-shell --master <master-url> --packages tech.ydb.spark:ydb-spark-connector-shaded:2.0.1 --conf spark.executor.memory=4g
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

- PySpark

  ```shell
  ~ $ pyspark --master <master-url> --packages tech.ydb.spark:ydb-spark-connector-shaded:2.0.1 --conf spark.executor.memory=4g
  Welcome to
        ____              __
      / __/__  ___ _____/ /__
      _\ \/ _ \/ _ `/ __/  '_/
    /__ / .__/\_,_/_/ /_/\_\   version 3.5.4
        /_/

  Using Python version 3.10.12 (main, May 27 2025 17:12:29)
  SparkSession available as 'spark'.
  ```

{% endlist %}

Let's display the schema of the Parquet file and count the number of rows it contains:

{% list tabs %}

- Spark Shell

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

- PySpark

  ```shell
  >>> so_posts2020 = spark.read.format("parquet").load("/home/username/2020.parquet")
  >>> so_posts2020.printSchema()
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

  >>> so_posts2020.count()
  4304021
  ```

{% endlist %}

Then add a new column with the year to this DataFrame and store it all to a column-oriented {{ ydb-short-name }} table:

{% list tabs %}

- Spark Shell

  ```shell
  scala> val my_ydb = Map("url" -> "grpcs://ydb.my-host.net:2135/preprod/spark-test?tokenFile=~/.token")
  my_ydb: scala.collection.immutable.Map[String,String] = Map(url -> grpcs://ydb.my-host.net:2135/preprod/spark-test?tokenFile=~/.token)

  scala> so_posts2020.withColumn("Year", lit(2020)).write.format("ydb").options(my_ydb).option("table.type", "column").option("table.primary_keys", "Id").mode("append").save("stackoverflow/posts");
  ```

- PySpark

  ```shell
  >>> from pyspark.sql.functions import col,lit
  >>> my_ydb = {"url": "grpcs://ydb.my-host.net:2135/preprod/spark-test?tokenFile=~/.token"}
  >>> so_posts2020.withColumn("Year", lit(2020)).write.format("ydb").options(**my_ydb).option("table.type", "column").option("table.primary_keys", "Id").mode("append").save("stackoverflow/posts")
  ```

{% endlist %}

As a result, you can read the stored data from the {{ ydb-short-name }} table and, for example, count the number of posts that have an accepted answer:

{% list tabs %}

- Spark Shell

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

- PySpark

  ```shell
  >>> ydb_posts2020 = spark.read.format("ydb").options(**my_ydb).load("stackoverflow/posts")
  >>> ydb_posts2020.printSchema()
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
  |-- Year: integer (nullable = true)

  >>> ydb_posts2020.count()
  4304021
  >>> ydb_posts2020.filter(col("AcceptedAnswerId") > 0).count()
  843780
  ```

{% endlist %}

## Spark SQL example {#example-spark-sql}

As an example, we'll show how to load a list of all Stack Overflow posts from 2020 into {{ ydb-short-name }}. This data can be downloaded from the following link: [https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/2020.parquet](https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/2020.parquet)

First, let's run `spark-sql` with the configured `my_ydb` catalog:

```shell
~ $ spark-sql --master <master-url> --packages tech.ydb.spark:ydb-spark-connector-shaded:2.0.1 \
     --conf spark.sql.catalog.my_ydb=tech.ydb.spark.connector.YdbCatalog \
     --conf spark.sql.catalog.my_ydb.url=grpcs://ydb.my-host.net:2135/preprod/spark-test \
     --conf spark.sql.catalog.my_ydb.auth.token.file=~/.token \
     --conf spark.executor.memory=4g
spark-sql (default)>
```

Let's validate the current state of the connected database and confirm the absence of the `stackoverflow/posts` table:

```shell
spark-sql (default)> SHOW NAMESPACES FROM my_ydb;
stackoverflow
Time taken: 0.11 seconds, Fetched 1 row(s)
spark-sql (default)> SHOW TABLES FROM my_ydb.stackoverflow;
Time taken: 0.041 seconds
```

Let's count the number of rows in the original parquet file:

```shell
spark-sql (default)> SELECT COUNT(*) FROM parquet.`/home/username/2020.parquet`;
4304021
```

Let's add a new column with the year and copy it all to a new {{ ydb-short-name }} table:

```shell
spark-sql (default)> CREATE TABLE my_ydb.stackoverflow.posts OPTIONS(table.primary_keys='Id') AS SELECT *, 2020 as Year FROM parquet.`/home/username/2020.parquet`;
Time taken: 85.225 seconds
```

Let's verify that the new table has appeared in the {{ ydb-short-name }} database:

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
```

As a result, we can read the stored data from {{ ydb-short-name }} table and, for example, count the number of posts that have an accepted answer:

```shell
spark-sql (default)> SELECT COUNT(*) FROM my_ydb.stackoverflow.posts;
4304021
Time taken: 19.726 seconds, Fetched 1 row(s)
spark-sql (default)> SELECT COUNT(*) FROM my_ydb.stackoverflow.posts WHERE AcceptedAnswerId > 0;
843780
Time taken: 6.599 seconds, Fetched 1 row(s)
```