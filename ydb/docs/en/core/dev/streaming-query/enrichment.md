# Data enrichment

**Enrichment** adds reference information to stream events. For example, an event may contain only an ID, while a lookup supplies a name or other attributes. The lookup can be a [local table](#enrichment-local-table) or [object storage on S3](#enrichment-s3).

In [streaming queries](../../concepts/streaming-query.md), you attach the lookup with `JOIN`: the stream must be on the left, the lookup on the right.

{% note warning %}

The lookup dataset is fully loaded into memory when the query starts. If the lookup data changes, restart the query — [DROP STREAMING QUERY](../../yql/reference/syntax/drop-streaming-query.md) and [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md) — to pick up a fresh snapshot.

{% endnote %}

## Preparing a data source for topics

Create an external data source for topics. Store the token in a [secret](../../yql/reference/syntax/create-secret.md) and create the source with [CREATE EXTERNAL DATA SOURCE](../../yql/reference/syntax/create-external-data-source.md).

```yql
-- Secret with a token for YDB access
CREATE SECRET `secrets/ydb_token` WITH (value = "<ydb_token>");

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

- `<ydb_endpoint>` is the {{ ydb-short-name }} endpoint, for example `grpcs://<ydb_host>:2135`.
- `<db_name>` is the path to the {{ ydb-short-name }} database, for example `/Root/database`.

## Streaming queries for enrichment

The examples below read events from an input topic, join each event with a service name from a lookup on `ServiceId`, and write results to an output topic.

Functions used in the queries:

- [TableRow](../../yql/reference/builtins/basic.md#tablerow)
- [Yson::From](../../yql/reference/udf/list/yson.md#ysonfrom)
- [Yson::SerializeJson](../../yql/reference/udf/list/yson.md#ysonserializejson)
- [Unwrap](../../yql/reference/builtins/basic.md#unwrap)
- [ToBytes](../../yql/reference/builtins/basic.md#to-from-bytes)

### Enrichment from a local table {#enrichment-local-table}

Here the lookup table `services_dict` lives in the current database as a regular [table](../../concepts/datamodel/table.md).

Create a [streaming query](../../concepts/streaming-query.md) that performs the enrichment:

```yql
CREATE STREAMING QUERY query_with_table_join AS
DO BEGIN

-- Read events from input topic
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

-- Join lookup to stream by ServiceId
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

The lookup is stored in S3 and connected through an [external data source](../../concepts/query_execution/federated_query/s3/external_data_source.md).

Create an additional [external data source](../../yql/reference/syntax/create-external-data-source.md) to read the lookup from S3:

```yql
-- S3 data source for the lookup
CREATE EXTERNAL DATA SOURCE s3_source WITH (
    SOURCE_TYPE = "ObjectStorage",
    LOCATION = "<s3_endpoint>",
    AUTH_METHOD = "NONE"
);
```

Where:

- `<s3_endpoint>` is the S3 URL, for example `https://storage.yandexcloud.net/<bucket>/` in Yandex Cloud.

Create a [streaming query](../../concepts/streaming-query.md) that performs the enrichment:

```yql
CREATE STREAMING QUERY query_with_join AS
DO BEGIN

-- Read events from input topic
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

-- Read service lookup from S3
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

-- Join lookup to stream by ServiceId
$joined_data = SELECT
    s.Name AS Name,
    t.*
FROM
    $topic_data AS t
LEFT JOIN
    $s3_data AS s
ON
    t.ServiceId = s.ServiceId;

-- Write JSON to output topic
INSERT INTO
    ydb_source.output_topic
SELECT
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
FROM
    $joined_data;

END DO
```

For more on data formats (`json_each_row`, `csv_with_names`, and others), see [{#T}](streaming-query-formats.md).
