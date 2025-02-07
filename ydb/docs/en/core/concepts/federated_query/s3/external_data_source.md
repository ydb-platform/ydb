# Working with S3 buckets ({{objstorage-full-name}})

To work with S3, you need to set up a data storage connection. There is a DDL for configuring such connections. Next, let's look at the SQL syntax and the management of these settings.

There are two types of buckets in S3: public and private. To connect to a public bucket, use `AUTH_METHOD="NONE"`. To connect to a private bucket, use `AUTH_METHOD="AWS"`. A detailed description of `AWS` can be found [here](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-authentication-methods.html). `AUTH_METHOD="NONE"` means that no authentication is used. If `AUTH_METHOD="AWS"` is specified, several additional parameters are required:

- `AWS_ACCESS_KEY_ID_SECRET_NAME` – reference to the name of the [secret](../../datamodel/secrets.md) where `AWS_ACCESS_KEY_ID` is stored.
- `AWS_SECRET_ACCESS_KEY_SECRET_NAME` – reference to the name of the [secret](../../datamodel/secrets.md) where `AWS_SECRET_ACCESS_KEY` is stored.
- `AWS_REGION` – region from which reading is performed, for example, `ru-central-1`.

To set up a connection to a public bucket, execute the following SQL query. The query creates an external connection named `object_storage`, which points to a specific S3 bucket named `bucket`.

```yql
CREATE EXTERNAL DATA SOURCE object_storage WITH (
  SOURCE_TYPE="ObjectStorage",
  LOCATION="https://object_storage_domain/bucket/",
  AUTH_METHOD="NONE"
);
```

To set up a connection to a private bucket, you need to run a few SQL queries. First, create [secrets](../../datamodel/secrets.md) containing `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

```yql
    CREATE OBJECT aws_access_id (TYPE SECRET) WITH (value=`<id>`);
    CREATE OBJECT aws_access_key (TYPE SECRET) WITH (value=`<key>`);
```

The next step is to create an external connection named `object_storage`, which points to a specific S3 bucket named `bucket` and uses `AUTH_METHOD="AWS"`. The parameters `AWS_ACCESS_KEY_ID_SECRET_NAME`, `AWS_SECRET_ACCESS_KEY_SECRET_NAME`, and `AWS_REGION` are filled in for `AWS`. The values of these parameters are described above.

```yql
CREATE EXTERNAL DATA SOURCE object_storage WITH (
  SOURCE_TYPE="ObjectStorage",
  LOCATION="https://object_storage_domain/bucket/",
  AUTH_METHOD="AWS",
  AWS_ACCESS_KEY_ID_SECRET_NAME="aws_access_id",
  AWS_SECRET_ACCESS_KEY_SECRET_NAME="aws_access_key",
  AWS_REGION="ru-central-1"
);
```

## Using an external connection to an S3 bucket {#external-data-source-settings}

When working with {{ objstorage-full-name }} using [external data sources](../../datamodel/external_data_source.md), it is convenient to perform prototyping and initial data connection setup.

An example query to read data:

```yql
SELECT
  *
FROM
 object_storage.`*.tsv`
WITH
(
  FORMAT = "tsv_with_names",
  SCHEMA =
  (
    ts Uint32,
    action Utf8
  )
);
```

The list of supported formats and data compression algorithms for reading data in S3 ({{objstorage-full-name}}) is provided in the section [{#T}](formats.md).

## Data model {#data_model}

In {{ objstorage-full-name }}, data is stored in files. To read data, you need to specify the data format in the files, compression, and lists of fields. This is done using the following SQL expression:

```yql
SELECT
  <expression>
FROM
  <object_storage_connection_name>.`<file_path>`
WITH(
  FORMAT = "<file_format>",
  COMPRESSION = "<compression>",
  SCHEMA = (<schema_definition>))
WHERE
  <filter>;
```

Where:

* `object_storage_connection_name` — the name of the external data source leading to the S3 bucket ({{ objstorage-full-name }}).
* `file_path` — the path to the file or files inside the bucket. Wildcards `*` are supported; more details [in the section](#path_format).
* `file_format` — the [data format](formats.md#formats) in the files.
* `compression` — the [compression format](formats.md#compression_formats) of the files.
* `schema_definition` — the [schema definition](#schema) of the data stored in the files.

### Data schema description {#schema}

The data schema description consists of a set of fields:

- Field name.
- Field type.
- Required data flag.

For example, the data schema below describes a schema field named `Year` of type `Int32` with the requirement that this field must be present in the data:

```text
Year Int32 NOT NULL
```

If a data field is marked as required (`NOT NULL`) but is missing in the processed file, processing such a file will result in an error. If a field is marked as optional (`NULL`), no error will occur the field is absent in the processed file, but the field will take the value `NULL`. The keyword `NULL` is optional in this context.

### Schema inference {#inference}

{{ ydb-short-name }} can determine the data schema of the files inside the bucket so that you do not have to specify these fields manually.

{% note info %}

Schema inference is available for all [data formats](formats.md#formats) except `raw` and `json_as_string`. For these formats you must [describe the schema manually](#schema).

{% endnote %}

To enable schema inference, use the `WITH_INFER` parameter:

```yql
SELECT
  <expression>
FROM
  <object_storage_connection_name>.`<file_path>`
WITH(
  FORMAT = "<file_format>",
  COMPRESSION = "<compression>",
  WITH_INFER = "true")
WHERE
  <filter>;
```

Where:

* `object_storage_connection_name` — the name of the external data source leading to the S3 bucket ({{ objstorage-full-name }}).
* `file_path` — the path to the file or files inside the bucket. Wildcards `*` are supported; For more information, see [{#T}](#path_format).
* `file_format` — the [data format](formats.md#formats) in the files. All formats except `raw` and `json_as_string` are supported.
* `compression` — the [compression format](formats.md#compression_formats) of the files.

As a result of executing such a query, the names and types of fields will be inferred.

### Data path formats {#path_format}

In {{ ydb-full-name }}, the following data paths are supported:

{% include [!](_includes/path_format.md) %}

## Example {#read_example}

Example query to read data from S3 ({{ objstorage-full-name }}):

```yql
SELECT
  *
FROM
  connection.`folder/filename.csv`
WITH(
  FORMAT = "csv_with_names",
  SCHEMA =
  (
    Year Int32,
    Manufacturer Utf8,
    Model Utf8,
    Price Double
  )
);
```

Where:

* `connection` — the name of the external data source leading to the S3 bucket ({{ objstorage-full-name }}).
* `folder/filename.csv` — the path to the file in the S3 bucket ({{ objstorage-full-name }}).
* `SCHEMA` — the data schema description in the file.