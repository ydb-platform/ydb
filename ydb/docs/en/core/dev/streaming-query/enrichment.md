# Data enrichment

Data enrichment is adding additional information from a reference table to events from a stream. For example, an event contains only an identifier, and the reference table allows you to add a name or other attributes to it. You can use data from a [local table](#enrichment-local-table) or from [S3 object storage](#enrichment-s3) as a reference table.

In [streaming queries](../../concepts/streaming-query/streaming-query.md), a reference table is connected using the `JOIN` construct. The stream must be on the left, and the reference table on the right.

{% note warning %}

The reference table is fully loaded into memory when the query starts. If the data in the reference table has changed, you need to restart the query to get the latest version of the reference table: delete it using [DROP STREAMING QUERY](../../yql/reference/syntax/drop-streaming-query.md) and create it again using [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md).

{% endnote %}

Data enrichment from [local and external topics](./local-and-external-topics.md) is possible.

In the examples below:

- `ext_source` — a pre-created [external data source](../../concepts/datamodel/external_data_source.md) for topics in another {{ ydb-short-name }} database;
- `input_topic` and `output_topic` — topics in the current or external {{ ydb-short-name }} database

## Streaming queries for data enrichment

The queries in the examples below read events from an input topic, join the service name from the reference table to each event by `ServiceId`, and write the result to an output topic.

For more information about the functions used in the queries:

- [TableRow](../../yql/reference/builtins/basic.md#tablerow)
- [Yson::From](../../yql/reference/udf/list/yson.md#ysonfrom)
- [Yson::SerializeJson](../../yql/reference/udf/list/yson.md#ysonserializejson)
- [Unwrap](../../yql/reference/builtins/basic.md#unwrap)
- [ToBytes](../../yql/reference/builtins/basic.md#to-from-bytes).

### Enrichment from a local table {#enrichment-local-table}

In this example, the reference table is stored in the [table](../../concepts/datamodel/table.md) `services_dict` in the current database.

Create a [streaming query](../../concepts/streaming-query/streaming-query.md) that performs enrichment:


```yql
CREATE STREAMING QUERY query_with_table_join AS
DO BEGIN

-- Reading events from the input topic
$topic_data = SELECT
    *
FROM
    ext_source.input_topic -- or local topic input_topic
WITH (
    FORMAT = json_each_row,
    SCHEMA = (
        Time String NOT NULL,
        ServiceId Uint32 NOT NULL,
        Message String NOT NULL
    )
);

-- Joining the reference table to the stream by ServiceId
$joined_data = SELECT
    s.Name AS Name,
    t.*
FROM
    $topic_data AS t
LEFT JOIN
    services_dict AS s
ON
    t.ServiceId = s.ServiceId;

-- Writing to the output topic (JSON)
INSERT INTO
    output_topic -- or external topic ext_source.output_topic
SELECT
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
FROM
    $joined_data;

END DO
```


### Enrichment from S3 {#enrichment-s3}

The reference table is stored in S3 and connected via an [external data source](../../concepts/query_execution/federated_query/s3/external_data_source.md).

Create an additional external data source to read the reference table from S3:


```yql
-- S3 data source for reading the reference table
CREATE EXTERNAL DATA SOURCE s3_source WITH (
    SOURCE_TYPE = "ObjectStorage",
    LOCATION = "<s3_endpoint>",
    AUTH_METHOD = "NONE"
);
```


Where:

- `<s3_endpoint>` — the S3 storage URL, for example `https://storage.yandexcloud.net/<bucket>/` for Yandex Cloud.

Create a [streaming query](../../concepts/streaming-query/streaming-query.md) that performs enrichment:


```yql
CREATE STREAMING QUERY query_with_join AS
DO BEGIN

-- Reading events from the input topic
$topic_data = SELECT
    *
FROM
    input_topic -- or external topic ext_source.input_topic
WITH (
    FORMAT = json_each_row,
    SCHEMA = (
        Time String NOT NULL,
        ServiceId Uint32 NOT NULL,
        Message String NOT NULL
    )
);

-- Reading the service reference table from S3
$s3_data = SELECT
    *
FROM
    s3_source.`file.csv`
WITH (
    FORMAT = csv_with_names,
    SCHEMA = (
        ServiceId Uint32,
        Name Utf8
    )
);

-- Joining the reference table to the stream by ServiceId
$joined_data = SELECT
    s.Name AS Name,
    t.*
FROM
    $topic_data AS t
LEFT JOIN
    $s3_data AS s
ON
    t.ServiceId = s.ServiceId;

-- Writing the result to the output topic in JSON format
INSERT INTO
    ext_source.output_topic -- or local topic output_topic
SELECT
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
FROM
    $joined_data;

END DO
```


For more information about data formats (`json_each_row`, `csv_with_names`, etc.): [{#T}](streaming-query-formats.md).
