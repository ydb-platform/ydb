# Data enrichment

Data enrichment is adding additional information from a reference to events from a stream. For example, an event contains only an identifier, and the reference allows adding a name or other attributes to it. As a reference, you can use data from a [local table](#enrichment-local-table) or from an [S3 object storage](#enrichment-s3).

In [streaming queries](../../concepts/streaming-query.md), the reference is connected using the `JOIN` construct. The stream must be on the left, the reference on the right.

{% note warning %}

The reference is fully loaded into memory when the query starts. If the data in the reference has changed, to get the current version of the reference, you need to restart the query — delete it using [DROP STREAMING QUERY](../../yql/reference/syntax/drop-streaming-query.md) and recreate it using [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md).

{% endnote %}

Enrichment works with [local and external topics](./local-and-external-topics.md).

In the examples below:

- `ext_source` — a pre-created [external data source](../../concepts/datamodel/external_data_source.md) for topics in another {{ ydb-short-name }} database;
- `input_topic` and `output_topic` — topics in the current or an external {{ ydb-short-name }} database.

## Streaming queries for data enrichment

The queries in the examples below read events from the input topic, attach the service name from the reference by `ServiceId` to each event, and write the result to the output topic.

More details about the functions used in the queries:

- [TableRow](../../yql/reference/builtins/basic.md#tablerow)
- [Yson::From](../../yql/reference/udf/list/yson.md#ysonfrom)
- [Yson::SerializeJson](../../yql/reference/udf/list/yson.md#ysonserializejson)
- [Unwrap](../../yql/reference/builtins/basic.md#unwrap)
- [ToBytes](../../yql/reference/builtins/basic.md#to-from-bytes).

### Enrichment from a local table {#enrichment-local-table}

In this example, the reference is stored in the [table](../../concepts/datamodel/table.md) `services_dict` in the current database.

Create a [streaming query](../../concepts/streaming-query.md) that performs enrichment:


```yql
CREATE STREAMING QUERY query_with_table_join AS
DO BEGIN

-- Read events from the input topic
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

-- Join the reference to the stream by ServiceId
$joined_data = SELECT
    s.Name AS Name,
    t.*
FROM
    $topic_data AS t
LEFT JOIN
    services_dict AS s
ON
    t.ServiceId = s.ServiceId;

-- Write to the output topic (JSON)
INSERT INTO
    output_topic -- or external topic ext_source.output_topic
SELECT
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
FROM
    $joined_data;

END DO
```


### Enrichment from S3 {#enrichment-s3}

The reference is stored in S3 and connected via an [external data source](../../concepts/query_execution/federated_query/s3/external_data_source.md).

Create an additional [external data source](../../yql/reference/syntax/create-external-data-source.md) to read the reference from S3:


```yql
-- S3 data source for reading the reference
CREATE EXTERNAL DATA SOURCE s3_source WITH (
    SOURCE_TYPE = "ObjectStorage",
    LOCATION = "<s3_endpoint>",
    AUTH_METHOD = "NONE"
);
```


Where:

- `<s3_endpoint>` — URL of the S3 storage, for example `https://storage.yandexcloud.net/<bucket>/` for Yandex Cloud.

Create a [streaming query](../../concepts/streaming-query.md) that performs enrichment:


```yql
CREATE STREAMING QUERY query_with_join AS
DO BEGIN

-- Read events from the input topic
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

-- Read the services reference from S3
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

-- Join the reference to the stream by ServiceId
$joined_data = SELECT
    s.Name AS Name,
    t.*
FROM
    $topic_data AS t
LEFT JOIN
    $s3_data AS s
ON
    t.ServiceId = s.ServiceId;

-- Write the result to the output topic as JSON
INSERT INTO
    ext_source.output_topic -- or local topic output_topic
SELECT
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
FROM
    $joined_data;

END DO
```


More details about data formats (`json_each_row`, `csv_with_names`, etc.): [{#T}](streaming-query-formats.md).
