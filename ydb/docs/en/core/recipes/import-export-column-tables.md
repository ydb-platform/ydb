# Import and export of data to column tables

Column tables in {{ ydb-short-name }} currently do not have a built-in backup and restore mechanism (it is in development). For data migration or recovery after failures, use export and import operations.

Two main approaches are available:

1. [Export and import via federated queries](#objstorage) to object storage (for example, {{ objstorage-name }} or any other S3-compatible storage).
**Advantages**: Uses built-in YDB functionality; no third-party services or tools required.
**Limitations**: This method only allows exporting data to object storage.

2. [Export and import via Apache Spark™](#spark) — a flexible approach for working with large volumes of data.
**Advantages**: Support for a wide range of target storage systems.
**Limitations**: Requires installation and configuration of additional software (Apache Spark™).

## Export and import via federated queries to {{ objstorage-name }} {#objstorage}

[Federated queries](../concepts/query_execution/federated_query/index.md) let {{ ydb-short-name }} read and write data directly in Parquet or CSV files. This method is convenient for running export and import operations using SQL only, without external tools.

### Prerequisites

- Object storage ({{ objstorage-name }}) and a static access key, for example [{{ objstorage-full-name }}](https://yandex.cloud/en/docs/storage/) with a pre-created bucket (for example, `your-bucket`).
- Network access from {{ ydb-short-name }} cluster nodes to the object storage. The example uses endpoint `storage.yandexcloud.net` — ensure access to it on port 443.
- The examples use TPC-H benchmark data. Instructions for loading test data are in the corresponding [section](../reference/ydb-cli/workload-tpch.md) of the guide.

### Create a secret for access to {{ objstorage-name }}

To connect to a private bucket, use authentication with static access keys. In {{ ydb-short-name }}, these keys are stored as [secrets](../concepts/datamodel/secrets.md).

```sql
CREATE SECRET aws_access_id WITH (value='<access_key_id>');
CREATE SECRET aws_access_key WITH (value='<secret_access_key>');
```

Where:

- `aws_access_id` — secret containing the access key ID.
- `<access_key_id>` — static access key identifier.
- `aws_access_key` — secret containing the secret key.
- `<secret_access_key>` — secret part of the access key.

### Configuring the connection

Next, configure the connection to the bucket by creating an external data source and an external table with a schema identical to `lineitem`.

```sql
-- Create a data source pointing to the bucket and using the secret
CREATE EXTERNAL DATA SOURCE `external/backup_datasource` WITH (
    SOURCE_TYPE="ObjectStorage",
    LOCATION="https://storage.yandexcloud.net/<bucket_name>/",
    AUTH_METHOD="AWS",
    AWS_ACCESS_KEY_ID_SECRET_PATH="aws_access_id",
    AWS_SECRET_ACCESS_KEY_SECRET_PATH="aws_access_key",
    AWS_REGION="ru-central1"
);
```

Where:

- `external/backup_datasource` — name of the external data source being created.
- `LOCATION` — bucket URL, including the bucket name `<bucket_name>`.
- `AUTH_METHOD="AWS"` — authentication method compatible with the S3 API.
- `AWS_ACCESS_KEY_ID_SECRET_PATH`, `AWS_SECRET_ACCESS_KEY_SECRET_PATH` — secrets used for authentication to {{ objstorage-name }}.

```sql
-- Create an external table with the lineitem schema
CREATE EXTERNAL TABLE `external/backup/lineitem_sql` (
    l_orderkey Int64 NOT NULL,
    l_partkey Int32 NOT NULL,
    l_suppkey Int32 NOT NULL,
    l_linenumber Int32 NOT NULL,
    l_quantity Double NOT NULL,
    l_extendedprice Double NOT NULL,
    l_discount Double NOT NULL,
    l_tax Double NOT NULL,
    l_returnflag String NOT NULL,
    l_linestatus String NOT NULL,
    l_shipdate Date NOT NULL,
    l_commitdate Date NOT NULL,
    l_receiptdate Date NOT NULL,
    l_shipinstruct String NOT NULL,
    l_shipmode String NOT NULL,
    l_comment String NOT NULL
) WITH (
    DATA_SOURCE="external/backup_datasource",
    LOCATION="/ydb-dumps-sql/lineitem/",
    FORMAT="parquet"
);
```

Where:

- `LOCATION` — path to the directory with data inside the bucket.
- `DATA_SOURCE` — name of the `EXTERNAL DATA SOURCE` object that contains the connection parameters.
- `external/backup/lineitem_sql` — full name of the external table being created.

### Exporting data from {{ ydb-short-name }}

To export data from table `tpch/s10/lineitem` to {{ objstorage-name }}, use `INSERT INTO ... SELECT` into the external table.

```sql
INSERT INTO `external/backup/lineitem_sql`
SELECT * FROM `tpch/s10/lineitem`;
```

After this query runs, Parquet files with the data will appear in bucket `your-bucket` at path `/ydb-dumps-sql/lineitem/`.

### Importing data into {{ ydb-short-name }}

{% note info %}

The INSERT command may fail if the table you are restoring data into already contains rows. In that case, clear the target table and run the INSERT command again.

{% endnote %}

To import data from {{ objstorage-name }} back into table `tpch/s10/lineitem`, use `INSERT INTO ... SELECT` from the external table.

```sql
INSERT INTO `tpch/s10/lineitem`
SELECT * FROM `external/backup/lineitem_sql`;
```

Here `tpch/s10/lineitem` is the name of the target table in {{ ydb-short-name }} into which the data will be loaded.

## Export and import with Apache Spark™ {#spark}

Using the [connector](../integrations/ingestion/spark.md) for {{ ydb-short-name }} and Apache Spark™ is a flexible and scalable way to export and import large volumes of data.

### Prerequisites

- PySpark version 4.0.1 installed; see the [installation guide](https://spark.apache.org/docs/latest/api/python/getting_started/install.html).
- A [gRPC endpoint](../concepts/connect.md#endpoint) for connecting to the {{ ydb-short-name }} database.
- [Access credentials](../reference/ydb-cli/connect.md#command-line-pars) for {{ ydb-short-name }} with read/write permissions.
- Network access from {{ ydb-short-name }} cluster nodes to the object storage. The example uses endpoint `storage.yandexcloud.net` — ensure access to it on port 443.
- The examples use TPC-H benchmark data. Instructions for loading test data are in the corresponding [section](../reference/ydb-cli/workload-tpch.md) of the guide.

### Exporting data from {{ ydb-short-name }} to Parquet

Parameters used:

- `spark.jars.packages` — Maven configuration parameter that loads the {{ ydb-short-name }} connector for Spark and other required components.
- `S3_ENDPOINT` — endpoint of the S3-compatible storage (for {{ objstorage-full-name }} use `https://storage.yandexcloud.net`).
- `S3_ACCESS_KEY` — static key ID for S3 access.
- `S3_SECRET_KEY` — secret part of the key for S3 access.
- `YDB_HOSTNAME` — gRPC endpoint host (for example, `ydb.serverless.yandexcloud.net`).
- `YDB_PORT` — gRPC endpoint port (for example, `2135`).
- `YDB_DATABASE_NAME` — path to your database (for example, `/ru-central1/b1g.../etn...`).
- `YDB_AUTH_TYPE` — parameters for authentication to {{ ydb-short-name }}, supported by the [Apache Spark driver](https://github.com/ydb-platform/ydb-spark-connector?tab=readme-ov-file#connector-usage).
- `YDB_SOURCE_TABLE` — path to the source table in the source database (for example, `tpch/s1/lineitem`).

```python
from pyspark.sql import SparkSession

# Source settings
YDB_HOSTNAME = ""
YDB_PORT = ""
YDB_DATABASE_NAME = ""
YDB_AUTH_TYPE = ""
YDB_SOURCE_TABLE = ""

# Destination settings
S3_ENDPOINT = ""
S3_ACCESS_KEY = ""
S3_SECRET_KEY = ""
S3_BUCKET_NAME = ""

spark = (SparkSession.builder
    .appName("ydb-export-lineitem-to-parquet")
    .config("spark.jars.packages", "tech.ydb.spark:ydb-spark-connector-shaded:2.0.1,org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.662")
    # S3 connector configuration
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")   # 30s
    .config("spark.hadoop.fs.s3a.connection.timeout", "200000")            # 200s
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")          # 60s
    .config("spark.hadoop.fs.s3a.connection.ttl", "300000")                # 5m
    .config("spark.hadoop.fs.s3a.assumed.role.session.duration", "1800000")# 30m
    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000")         # 24h
    .config("spark.hadoop.fs.s3a.retry.interval", "500")                   # 500ms
    .config("spark.hadoop.fs.s3a.retry.throttle.interval", "100")          # 100ms
    .getOrCreate())

# Read data from the lineitem table
df = (spark.read.format("ydb")
    .option("url", f"grpcs://{YDB_HOSTNAME}:{YDB_PORT}{YDB_DATABASE_NAME}?{YDB_AUTH_TYPE}")
    .load(YDB_SOURCE_TABLE))

# Write data to Parquet files in S3
(df.repartition(64)
    .write.mode("overwrite")
    .option("compression", "snappy")
    .parquet(f"s3a://{S3_BUCKET_NAME}/ydb-dumps-spark/lineitem/"))

spark.stop()
```

### Importing data from Parquet into {{ ydb-short-name }}

- `spark.jars.packages` — Maven configuration parameter that loads the {{ ydb-short-name }} connector for Spark and other required components.
- `S3_ENDPOINT` — endpoint of the S3-compatible storage (for {{ objstorage-full-name }} use `https://storage.yandexcloud.net`).
- `S3_ACCESS_KEY` — static key ID for S3 access.
- `S3_SECRET_KEY` — secret part of the key for S3 access.
- `YDB_HOSTNAME` — gRPC endpoint host (for example, `ydb.serverless.yandexcloud.net`).
- `YDB_PORT` — gRPC endpoint port (for example, `2135`).
- `YDB_DATABASE_NAME` — path to your database (for example, `/ru-central1/b1g.../etn...`).
- `YDB_AUTH_TYPE` — parameters for authentication to {{ ydb-short-name }}, supported by the [Apache Spark driver](https://github.com/ydb-platform/ydb-spark-connector?tab=readme-ov-file#connector-usage).
- `YDB_TARGET_TABLE` — path to the table in the destination database (for example, `tpch/s1/lineitem`).

```python
from pyspark.sql import SparkSession

# Source settings
S3_ENDPOINT = "https://storage.yandexcloud.net"
S3_ACCESS_KEY = ""
S3_SECRET_KEY = ""
S3_BUCKET_NAME = ""
S3_FOLDER_PATH = ""

# Destination settings
YDB_HOSTNAME = ""
YDB_PORT = ""
YDB_DATABASE_NAME = ""
YDB_AUTH_TYPE = ""
YDB_TARGET_TABLE = ""

spark = (SparkSession.builder
    .appName("ydb-import-lineitem-from-parquet")
    .config("spark.jars.packages", "tech.ydb.spark:ydb-spark-connector-shaded:2.0.1,org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.662")
    # S3 connector configuration (same as for export)
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")   # 30s
    .config("spark.hadoop.fs.s3a.connection.timeout", "200000")            # 200s
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")          # 60s
    .config("spark.hadoop.fs.s3a.connection.ttl", "300000")                # 5m
    .config("spark.hadoop.fs.s3a.assumed.role.session.duration", "1800000")# 30m
    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000")         # 24h
    .config("spark.hadoop.fs.s3a.retry.interval", "500")                   # 500ms
    .config("spark.hadoop.fs.s3a.retry.throttle.interval", "100")          # 100ms
    .getOrCreate())

# Read data from Parquet files in S3 created during the export step
df = spark.read.parquet(f"s3a://{S3_BUCKET_NAME}/{S3_FOLDER_PATH}")

# Write data to the target table
(df.write.format("ydb")
    .option("url", f"grpcs://{YDB_HOSTNAME}:{YDB_PORT}{YDB_DATABASE_NAME}?{YDB_AUTH_TYPE}")
    .mode("append")
    .save(YDB_TARGET_TABLE))

spark.stop()
```
