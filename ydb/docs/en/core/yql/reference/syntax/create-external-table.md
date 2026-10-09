# CREATE EXTERNAL TABLE

The `CREATE EXTERNAL TABLE` statement creates an [external table](../../../concepts/datamodel/external_table.md) with the given schema.

```yql
CREATE EXTERNAL TABLE table_name (
  column1 type1,
  column2 type2 NOT NULL,
  ...
  columnN typeN NULL
) WITH (
  DATA_SOURCE="data_source_name",
  LOCATION="path",
  FORMAT="format_name",
  COMPRESSION="compression_name"
);
```

Where:

* `column1 type1`, `columnN typeN NULL` — column definitions and types;
* `data_source_name` — name of the [connection](../../../concepts/datamodel/external_data_source.md) to S3 ({{ objstorage-name }}).
* `path` — a file path, a directory prefix ending in `/`, or a wildcard pattern inside the data bucket.
* `format_name` — one of the [supported storage formats](../../../concepts/query_execution/federated_query/s3/formats.md).
* `compression_name` — one of the [supported compression algorithms](../../../concepts/query_execution/federated_query/s3/formats.md#compression).

`VALIDATE_EXTERNAL` controls all additional checks against the external system when creating or replacing an external table. Set it to `"true"` in `WITH` to run the checks supported by the data source, or to `"false"` to skip them. The default is `"false"`, so automation can create tables before external resources or data become available. Required local checks, including the table schema and option syntax, still apply. Subsequent queries perform their usual checks.

For S3, `VALIDATE_EXTERNAL="true"` currently checks that `LOCATION` exists using the external data source's credentials. This requires permission to list the bucket. A missing bucket, a missing file or directory prefix, a pattern without matching files, or an access error prevents table creation or replacement. Errors include the location and bucket address.

An empty bucket is valid when `LOCATION="/"`. An empty directory is valid if S3 contains a directory marker object (a key ending in `/`); a prefix with no objects or directory markers does not exist in S3. The check validates `LOCATION`, independently of `FILE_PATTERN`, partition projection, and file contents.

This is a creation-time check; it does not guarantee that the location will remain available.

Only a limited subset of data types is allowed:

- `Bool`.
- `Int8`, `Uint8`, `Int16`, `Uint16`, `Int32`, `Uint32`, `Int64`, `Uint64`.
- `Float`, `Double`.
- `Date`, `DateTime`.
- `String`, `Utf8`.

Without extra modifiers, a column has an [optional type](../types/optional.md) and may contain `NULL`. Use `NOT NULL` for a non-optional column.

## Example

The following creates an external table `s3_test_data` with string columns `key` and `value` in CSV format under `test_folder` in the bucket, using external data source `bucket`:

```yql
CREATE EXTERNAL TABLE s3_test_data (
  key Utf8 NOT NULL,
  value Utf8 NOT NULL
) WITH (
  DATA_SOURCE="bucket",
  LOCATION="folder/",
  FORMAT="csv_with_names",
  COMPRESSION="gzip"
);
```
