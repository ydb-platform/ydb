# Common streaming query patterns

This section contains minimal [streaming query](../../concepts/streaming-query.md) examples for the most common scenarios. It starts with a basic read from a topic, then full processing patterns: transform data and write results to a topic as JSON, to a topic as a string, and to a table. Each example can be used as a starting point for your own tasks.

## Reading from a topic {#topic-read}

Read from a topic with `SELECT ... FROM ... WITH (FORMAT, SCHEMA)`. The `WITH` block specifies the input format and schema — the fields expected in each message and their types. This pattern is used in all following examples.

{% note info %}

Topics are accessed through an [external data source](../../concepts/datamodel/external_data_source.md).

In the examples:

- `ydb_source` is a pre-created `external data source`;
- `input_topic` is the topic read from;
- `output_topic` is the topic written to;
- `output_table` is the {{ ydb-short-name }} table written to.

{% endnote %}

The following fragment reads events from a topic in JSON format. It is used inside [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md) in a `DO BEGIN ... END DO` block:

```yql
SELECT
    *
FROM
    ydb_source.input_topic
WITH (
    FORMAT = json_each_row,
    SCHEMA = (
        Id Uint64 NOT NULL,
        Name Utf8 NOT NULL
    )
);
```

For more on formats, see [{#T}](streaming-query-formats.md).

## Writing to a topic (JSON) {#topic-json}

The query reads events from the input topic, builds a JSON object from fields, and writes to the output topic. `AsStruct` builds a struct from the specified fields, `Yson::From` converts it to Yson, `Yson::SerializeJson` serializes to a JSON string, and `ToBytes` converts to `String` for topic writes.

```yql
CREATE STREAMING QUERY write_json_example AS
DO BEGIN

-- ydb_source: external data source for topics
INSERT INTO ydb_source.output_topic
SELECT
    -- Build JSON from individual fields
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(
        AsStruct(Id AS id, Name AS name)
    ))))
FROM
    ydb_source.input_topic
WITH (
    FORMAT = json_each_row,  -- Input format
    SCHEMA = (               -- Input schema
        Id Uint64 NOT NULL,
        Name Utf8 NOT NULL
    )
);

END DO
```

See also:

- [AsStruct](../../yql/reference/builtins/basic#as-container)
- [Yson::From](../../yql/reference/udf/list/yson#ysonfrom)
- [Yson::SerializeJson](../../yql/reference/udf/list/yson#ysonserializejson)
- [Unwrap](../../yql/reference/builtins/basic#unwrap)
- [ToBytes](../../yql/reference/builtins/basic#to-from-bytes)

## Writing to a topic (string) {#topic-utf8}

The query reads events from the input topic and writes a single string column to the output topic. For string topic writes, `SELECT` must return a single column of type `String` or `Utf8`.

```yql
CREATE STREAMING QUERY write_utf8_example AS
DO BEGIN

-- ydb_source: external data source for topics
INSERT INTO ydb_source.output_topic
SELECT
    Name
FROM
    ydb_source.input_topic
WITH (
    FORMAT = json_each_row,  -- Input format
    SCHEMA = (               -- Input schema
        Id Uint64 NOT NULL,
        Name Utf8 NOT NULL
    )
);

END DO
```

For more on write formats, see [{#T}](streaming-query-formats.md#write_formats).

## Writing to a table {#table-write}

The query reads events from a topic and writes to `output_table`. The table must be created in advance with a schema matching the selected columns.

{% note warning %}

Table writes in streaming queries are supported **only in UPSERT mode**. `INSERT INTO` is not supported: with [at-least-once](../../concepts/streaming-query.md#guarantees) delivery, reprocessing would duplicate rows. With `UPSERT`, an existing row with the same primary key is updated; otherwise a new row is inserted, while `INSERT INTO` would error on duplicates.

{% endnote %}

```yql
CREATE STREAMING QUERY write_table_example AS
DO BEGIN

-- Table write (UPSERT only; INSERT is not supported)
UPSERT INTO output_table
SELECT
    Id,
    Name
FROM
    -- ydb_source: external data source for topics
    ydb_source.input_topic
WITH (
    FORMAT = json_each_row,  -- Input format
    SCHEMA = (               -- Input schema
        Id Uint64 NOT NULL,
        Name Utf8 NOT NULL
    )
);

END DO
```

For more information, see [{#T}](table-writing.md).

## See also

- [{#T}](../../yql/reference/syntax/create-streaming-query.md)
- [{#T}](../../recipes/streaming_queries/topics.md)
