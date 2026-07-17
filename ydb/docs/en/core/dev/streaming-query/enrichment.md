# Data enrichment

<<<<<<< HEAD
**Data enrichment** means attaching additional information from a lookup to events in the stream. For example, an event may only contain an ID, while a lookup provides a name or other attributes. Lookups can come from a [local table](#enrichment-local-table) or from [S3 object storage](#enrichment-s3).

In [streaming queries](../../concepts/streaming-query.md), you attach a lookup with `JOIN`. The stream must be on the left, the lookup on the right.

{% note warning %}

The entire lookup is loaded into memory when the query starts. If the lookup data changes, restart the query to pick up fresh data — delete it with [DROP STREAMING QUERY](../../yql/reference/syntax/drop-streaming-query.md) and create it again with [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md).

{% endnote %}

## Prepare a data source for topics
=======
Data enrichment is adding additional information from a reference to events in a stream. For example, an event contains only an identifier, and the reference allows adding a name or other attributes to it. You can use data from a [local table](#enrichment-local-table) or from an [S3 object storage](#enrichment-s3) as a reference.

In streaming queries, the reference is connected using the `JOIN` construct. The stream must be on the left, the reference on the right.

{% note warning %}

The reference is fully loaded into memory when the query starts. If the data in the reference has changed, to get the current version of the reference, you need to restart the query — delete it using [DROP STREAMING QUERY](../../yql/reference/syntax/drop-streaming-query.md) and recreate it using [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md).

{% endnote %}

Data enrichment from [local and external topics](../../concepts/query_execution/topics.md#local-external-topics) is possible.
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))

Create an external data source for topics. Store tokens in a secret and create the source with CREATE EXTERNAL DATA SOURCE.

<<<<<<< HEAD
```yql
-- Secret with token for YDB
CREATE SECRET `secrets/ydb_token` WITH (value = "<ydb_token>");
=======
- `ext_source` — a pre-created [external data source](../../concepts/datamodel/external_data_source.md) for topics in another {{ ydb-short-name }} database
- `input_topic` and `output_topic` — topics in the current or external {{ ydb-short-name }} database
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))

-- YDB data source for reading/writing topics
CREATE EXTERNAL DATA SOURCE ydb_source WITH (
    SOURCE_TYPE = "Ydb",
    LOCATION = "<ydb_endpoint>",
    DATABASE_NAME = "<db_name>",
    AUTH_METHOD = "TOKEN",
    TOKEN_SECRET_PATH = "secrets/ydb_token"
);
```

Where:

- `<ydb_endpoint>` — {{ ydb-short-name }} endpoint, for example `grpcs://<ydb_host>:2135`.
- `<db_name>` — path to the {{ ydb-short-name }} database, for example `/Root/database`.

## Streaming queries for enrichment

The queries in the examples below read events from the input topic, attach the service name from the reference by `ServiceId` to each event, and write the result to the output topic.

For more details on the functions used in the queries:

- [TableRow](../../yql/reference/builtins/basic.md#tablerow)
- [Yson::From](../../yql/reference/udf/list/yson.md#ysonfrom)
- [Yson::SerializeJson](../../yql/reference/udf/list/yson.md#ysonserializejson)
- [Unwrap](../../yql/reference/builtins/basic.md#unwrap)
- [ToBytes](../../yql/reference/builtins/basic.md#to-from-bytes).

### Enrichment from a local table {#enrichment-local-table}

<<<<<<< HEAD
Here the lookup is stored in table `services_dict` in the current database ([table](../../concepts/datamodel/table.md)).

Create a [streaming query](../../concepts/streaming-query.md) that performs the enrichment:
=======
In this example, the reference is stored in the [table](../../concepts/datamodel/table.md) `services_dict` in the current database.

Create a streaming query that performs enrichment:

>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))

```yql
CREATE STREAMING QUERY query_with_table_join AS
DO BEGIN

<<<<<<< HEAD
-- Read events from input topic
=======
-- Reading events from input topic
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))
$topic_data = SELECT
    *
FROM
    ydb_source.input_topic
WITH (
    FORMAT = json_each_row,
    SCHEMA = (
        Time String NOT NULL,
        ServiceId Uint32 NOT NULL,
        Message String NOT NULL
    )
);

<<<<<<< HEAD
-- Join lookup to stream on ServiceId
=======
-- Joining reference to stream by ServiceId
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))
$joined_data = SELECT
    s.Name AS Name,
    t.*
FROM
    $topic_data AS t
LEFT JOIN
    services_dict AS s
ON
    t.ServiceId = s.ServiceId;

-- Write to output topic (JSON)
INSERT INTO
    ydb_source.output_topic
SELECT
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
FROM
    $joined_data;

END DO
```

### Enrichment from S3 {#enrichment-s3}

<<<<<<< HEAD
The lookup is stored in S3 and connected through an [external data source](../../concepts/query_execution/federated_query/s3/external_data_source.md).

Create another external data source to read the lookup from S3:

```yql
-- S3 data source for lookup data
=======
The reference is stored in S3 and connected via an [external data source](../../concepts/query_execution/federated_query/s3/external_data_source.md).

Create an additional external data source to read the reference from S3:


```yql
-- S3 data source for reading reference
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))
CREATE EXTERNAL DATA SOURCE s3_source WITH (
    SOURCE_TYPE = "ObjectStorage",
    LOCATION = "<s3_endpoint>",
    AUTH_METHOD = "NONE"
);
```

Where:

<<<<<<< HEAD
- `<s3_endpoint>` — S3 URL, for example `https://storage.yandexcloud.net/<bucket>/` in Yandex Cloud.

Create a [streaming query](../../concepts/streaming-query.md) that performs the enrichment:
=======
- `<s3_endpoint>` — the S3 storage URL, for example `https://storage.yandexcloud.net/<bucket>/` for Yandex Cloud.

Create a streaming query that performs enrichment:

>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))

```yql
CREATE STREAMING QUERY query_with_join AS
DO BEGIN

<<<<<<< HEAD
-- Read events from input topic
=======
-- Reading events from input topic
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))
$topic_data = SELECT
    *
FROM
    ydb_source.input_topic
WITH (
    FORMAT = json_each_row,
    SCHEMA = (
        Time String NOT NULL,
        ServiceId Uint32 NOT NULL,
        Message String NOT NULL
    )
);

<<<<<<< HEAD
-- Read service lookup from S3
=======
-- Reading service reference from S3
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))
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

<<<<<<< HEAD
-- Join lookup to stream on ServiceId
=======
-- Joining reference to stream by ServiceId
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))
$joined_data = SELECT
    s.Name AS Name,
    t.*
FROM
    $topic_data AS t
LEFT JOIN
    $s3_data AS s
ON
    t.ServiceId = s.ServiceId;

<<<<<<< HEAD
-- Write JSON to output topic
=======
-- Write result to output topic in JSON format
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))
INSERT INTO
    ydb_source.output_topic
SELECT
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
FROM
    $joined_data;

END DO
```

<<<<<<< HEAD
For supported data formats (`json_each_row`, `csv_with_names`, etc.), see [{#T}](streaming-query-formats.md).
=======

For more details on data formats (`json_each_row`, `csv_with_names`, etc.): [{#T}](streaming-query-formats.md).
>>>>>>> ac24e5289e4 (Auto-translate docs from PR #39856 (#46871))
