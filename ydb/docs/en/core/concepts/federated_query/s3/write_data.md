# Writing data to S3 buckets ({{ objstorage-full-name }})

In {{ ydb-full-name }}, you can use [external connections](#connection-write) or [external tables](#external-table-write) to write data to the {{ objstorage-full-name }} bucket.

## Writing data via external connection {#connection-write}

Using connections for data writing is convenient for prototyping and initial setup. The SQL expression demonstrates writing data directly to an external data source.

```sql
INSERT INTO `connection`.`test/`
WITH
(
  FORMAT = "csv_with_names"
)
SELECT
    "value" AS value, "name" AS name
```

The data will be written to the specified path. In this mode, the resulting files will not be partitioned. If you need to partition the resulting files, use writing via [external tables](#external-table-write). The files created during the writing process are assigned random names.

When working with external connections, only read (`SELECT`) and insert (`INSERT`) operations are possible; other types of operations are not supported.

## Writing data via external tables {#external-table-write}

If you need to write data regularly, doing this using external tables is convenient. In this case, there is no need to specify all the details of working with this data in each query. To write data to the bucket, create an [external table](external_table.md) in S3 ({{ objstorage-full-name }}) and use the usual SQL `INSERT INTO` statement:

```sql
INSERT INTO `test`
SELECT
    "value" AS value, "name" AS name
```