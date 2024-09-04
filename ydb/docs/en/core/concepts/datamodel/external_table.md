# External tables

{% note warning %}

This functionality is in "Preview" mode.

{% endnote %}

Some [external data sources](external_data_source.md), such as database management systems, store data in a structured format, while others, like S3 ({{objstorage-full-name}}), store data as individual files. To work with file-based data sources, you need to understand both the file placement rules and the formats of the stored data.

A special entity, `EXTERNAL TABLE,` describes the stored data in such sources. External tables allow you to define the schema of the stored files and the schema of file placement within the source.

A record in YQL might look like this:

```sql
CREATE EXTERNAL TABLE s3_test_data (
  key Utf8 NOT NULL,
  value Utf8 NOT NULL
) WITH (
  DATA_SOURCE="bucket",
  LOCATION="folder",
  FORMAT="csv_with_names",
  COMPRESSION="gzip"
);
```

Data can be inserted into external tables just like regular tables. For example, to write data to an external table, you need to execute the following query:

```sql
INSERT INTO s3_test_data
SELECT * FROM Table
```

More details on working with external tables describing S3 buckets ({{ objstorage-name }}) can be found in section [{#T}](../federated_query/s3/external_table.md).