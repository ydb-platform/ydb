# Common streaming query patterns

This section collects minimal examples of [streaming queries](../../concepts/streaming-query.md) for typical scenarios. It starts with a basic topic read, then shows end-to-end processing: handling data and writing results to a topic as JSON, to a topic as a plain string, and to a table. Each example can be used as a starting point for your own workloads.

## Reading from a topic {#topic-read}

Read from a topic using `SELECT ... FROM ... WITH (FORMAT, SCHEMA)`. The `WITH` block specifies the input format and schema—the fields expected in each message and their types. This pattern appears in all examples below.

{% note info %}

Topics are accessed through an [external data source](../../concepts/datamodel/external_data_source.md).

In the examples:

- `ydb_source` — a pre-created external data source;
- `input_topic` — topic to read from;
- `output_topic` — topic to write results to;
- `output_table` — {{ ydb-short-name }} table to write results to.

{% endnote %}

The following snippet reads JSON events from a topic. Use it inside [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md) in a `DO BEGIN ... END DO` block:

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

The query reads events from the input topic, builds a JSON object from fields, and writes to the output topic. `AsStruct` builds a structure from the fields, `Yson::From` converts it to Yson, `Yson::SerializeJson` serializes to a JSON string, and `ToBytes` converts to `String`, which is required for topic writes.

```yql
CREATE STREAMING QUERY write_json_example AS
DO BEGIN

-- ydb_source — external data source for topics
INSERT INTO ydb_source.output_topic
SELECT
    -- Build JSON from fields
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(
        AsStruct(Id AS id, Name AS name)
    ))))
FROM
    ydb_source.input_topic
WITH (
    FORMAT = json_each_row,  -- Input data format
    SCHEMA = (               -- Input schema
        Id Uint64 NOT NULL,
        Name Utf8 NOT NULL
    )
);

END DO
```

More on the functions:

- [AsStruct](../../yql/reference/builtins/basic#as-container)
- [Yson::From](../../yql/reference/udf/list/yson#ysonfrom)
- [Yson::SerializeJson](../../yql/reference/udf/list/yson#ysonserializejson)
- [Unwrap](../../yql/reference/builtins/basic#unwrap)
- [ToBytes](../../yql/reference/builtins/basic#to-from-bytes).

## Writing to a topic (string) {#topic-utf8}

The query reads events from the input topic and writes a single field as a string to the output topic. Topic writes require `SELECT` to return a single column of type `String` or `Utf8`.

```yql
CREATE STREAMING QUERY write_utf8_example AS
DO BEGIN

-- ydb_source — external data source for topics
INSERT INTO ydb_source.output_topic
SELECT
    Name
FROM
    ydb_source.input_topic
WITH (
    FORMAT = json_each_row,  -- Input data format
    SCHEMA = (               -- Input schema
        Id Uint64 NOT NULL,
        Name Utf8 NOT NULL
    )
);

END DO
```

More on write formats: [{#T}](streaming-query-formats.md#write_formats).

## Writing to a table {#table-write}

The query reads events from a topic and writes them to `output_table`. Create the table beforehand with a schema that matches the selected columns.

{% note warning %}

Table writes in streaming queries support **UPSERT only**. `INSERT INTO` is not supported: with [at-least-once](../../concepts/streaming-query.md#guarantees) delivery, retries would duplicate rows. With `UPSERT`, an existing row with the same primary key is updated; otherwise a new row is inserted, while `INSERT INTO` fails.

{% endnote %}

```yql
CREATE STREAMING QUERY write_table_example AS
DO BEGIN

-- Write to table (UPSERT only; INSERT is not supported)
UPSERT INTO output_table
SELECT
    Id,
    Name
FROM
    -- ydb_source — external data source for topics
    ydb_source.input_topic
WITH (
    FORMAT = json_each_row,  -- Input data format
    SCHEMA = (               -- Input schema
        Id Uint64 NOT NULL,
        Name Utf8 NOT NULL
    )
);

END DO
```

More details: [{#T}](table-writing.md).

## See also

- [{#T}](../../yql/reference/syntax/create-streaming-query.md)
- [{#T}](../../recipes/streaming_queries/topics.md)
