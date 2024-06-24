# Working with S3 buckets ({{objstorage-full-name}})

When working with {{ objstorage-full-name }} using [external data sources](../../datamodel/external_data_source.md), it is convenient to perform prototyping and initial data connection setup.

An example query to read data:

```sql
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

```sql
SELECT
  <expression>
FROM
  <object_storage_connection_name>.`<file_path>`
WITH(
 FORMAT = "<file_format>",
  SCHEMA = (<schema_definition>),
  COMPRESSION = "<compression>")
WHERE
  <filter>;
```

Where:

* `object_storage_connection_name` — the name of the [external data source](#create_connection) leading to the S3 bucket ({{ objstorage-full-name }}).
* `file_path` — the path to the file or files inside the bucket. Wildcards `*` are supported; more details [in the section](#path_format).
* `file_format` — the [data format](formats.md#formats) in the files.
* `schema_definition` — the [schema definition](#schema) of the data stored in the files.
* `compression` — the [compression format](formats.md#compression_formats) of the files.

### Data schema description {#schema}

The data schema description consists of a set of fields:
- Field name.
- Field type.
- Required data flag.

For example, the data schema below describes a schema field named `Year` of type `Int32` with the requirement that this field must be present in the data:

```
Year Int32 NOT NULL
```

If a data field is marked as required (`NOT NULL`) but this field is missing in the processed file, the processing of such a file will result in an error. If a field is marked as optional (`NULL`), no error will occur in the absence of the field in the processed file, but the field will take the value `NULL`.

### Data path formats {#path_format}

In {{ ydb-full-name }}, the following data paths are supported:

{% include [!](_includes/path_format.md) %}

## Example {#read_example}

Example query to read data from S3 ({{ objstorage-full-name }}):

```sql
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