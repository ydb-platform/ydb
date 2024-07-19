# Reading data from an external table pointing to S3 ({{ objstorage-name }})

Sometimes, the same data queries need to be executed regularly. To avoid specifying all the details of working with this data every time a query is called, use the mode with [external tables](../../datamodel/external_table.md). In this case, the query looks like a regular query to {{ydb-full-name}} tables.

Example query for reading data:

```sql
SELECT
    *
FROM
    `s3_test_data`
WHERE
    version > 1
```

## Creating an external table pointing to an S3 bucket ({{ objstorage-name }}) {#external-table-settings}

To create an external table describing the S3 bucket ({{ objstorage-name }}), execute the following SQL query. The query creates an external table named `s3_test_data`, containing files in the `CSV` format with string fields `key` and `value`, located inside the bucket at the path `test_folder`, using the connection credentials specified by the [external data source](../../datamodel/external_data_source.md) object `bucket`:

```sql
CREATE EXTERNAL TABLE `s3_test_data` (
  key Utf8 NOT NULL,
  value Utf8 NOT NULL
) WITH (
  DATA_SOURCE="bucket",
  LOCATION="folder",
  FORMAT="csv_with_names",
  COMPRESSION="gzip"
);
```

Where:
- `key, value` - list of data columns and their types;
- `bucket` - name of the [external data source](../../datamodel/external_data_source.md) to S3 ({{ objstorage-name }});
- `folder` - path within the bucket containing the data;
- `csv_with_names` - one of the [permitted data storage formats](formats.md);
- `gzip` - one of the [permitted compression algorithms](formats.md#compression).

## Data model {#data-model}

Reading data using external tables from S3 ({{ objstorage-name }}) is done with regular SQL queries as if querying a normal table.

```sql
SELECT
    <expression>
FROM
    `s3_test_data`
WHERE
    <filter>;
```

## Limitations

There are a number of limitations when working with S3 buckets ({{ objstorage-name }}).

Limitations:
1. Only data read requests - `SELECT` and `INSERT` are supported; other requests are not.
1. {% include [!](../_includes/datetime_limits.md)%}