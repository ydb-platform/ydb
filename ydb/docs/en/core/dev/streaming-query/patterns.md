# Typical streaming query patterns

This section contains minimal examples of streaming queries for the most common scenarios. First, a basic pattern for reading data from a topic is described, followed by options for full data processing: data processing and writing results to a topic in JSON format, to a topic as a string, and to a table. Each example can be used as a starting point for your own tasks.

The examples below use [local and external topics](../../concepts/query_execution/topics.md#local-external-topics). Notation:

- `ext_source` — a pre-created [`external data source`](../../concepts/datamodel/external_data_source.md).
- `input_topic` — the topic from which data is read.
- `output_topic` — the topic where results are written.
- `output_table` — the {{ ydb-short-name }} table where results are written.

## Reading data from a topic {#topic-read}

Reading structured messages is done using `SELECT ... FROM ... WITH (FORMAT, SCHEMA)`. The `WITH` block specifies the input data format and schema — which fields are expected in each message and their types.

The following snippet is used inside [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md) in the `DO BEGIN ... END DO` block:


```yql
SELECT
    Id,
    Name
FROM
    topic_name  -- local topic; for external: ext_source.topic_name
WITH (
    FORMAT = json_each_row,
    SCHEMA = (
        Id Uint64 NOT NULL,
        Name Utf8 NOT NULL
    )
);
```


For more details on formats: [{#T}](streaming-query-formats.md).

This pattern is used in all subsequent examples.

## Writing to a topic (JSON) {#topic-json}

The query reads events from the input topic, creates a JSON object from individual fields, and writes the result to the output topic. The `AsStruct` function creates a structure from the specified fields, `Yson::From` converts it to Yson, `Yson::SerializeJson` serializes it to a JSON string, and `ToBytes` converts it to the `String` type required for writing to the topic.


```yql
CREATE STREAMING QUERY write_json_example AS
DO BEGIN

INSERT INTO ext_source.output_topic -- or local topic output_topic
SELECT
    -- Forming JSON from individual fields
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(
        AsStruct(Id AS id, Name AS name)
    ))))
FROM
    input_topic -- or external topic ext_source.input_topic
WITH (
    FORMAT = json_each_row,  -- Input data format
    SCHEMA = (               -- Input data schema
        Id Uint64 NOT NULL,
        Name Utf8 NOT NULL
    )
);

END DO
```


For more details on functions:

- [AsStruct](../../yql/reference/builtins/basic#as-container)
- [Yson::From](../../yql/reference/udf/list/yson#ysonfrom)
- [Yson::SerializeJson](../../yql/reference/udf/list/yson#ysonserializejson)
- [Unwrap](../../yql/reference/builtins/basic#unwrap)
- [ToBytes](../../yql/reference/builtins/basic#to-from-bytes).

## Writing to a topic (string) {#topic-utf8}

The query reads events from the input topic and writes a single field as a string to the output topic. To write strings to a topic, `SELECT` must return a single column of type `String` or `Utf8`.


```yql
CREATE STREAMING QUERY write_utf8_example AS
DO BEGIN

INSERT INTO output_topic -- or external topic ext_source.output_topic
SELECT
    Name
FROM
    ext_source.input_topic -- or local topic input_topic
WITH (
    FORMAT = json_each_row,  -- Input data format
    SCHEMA = (               -- Input data schema
        Id Uint64 NOT NULL,
        Name Utf8 NOT NULL
    )
);

END DO
```


For more details on write formats: [{#T}](streaming-query-formats.md#write_formats).

## Writing to a table {#table-write}

The query reads events from the topic and writes them to the `output_table` table. The table must be created in advance with a schema matching the selected columns.

{% note warning %}

Writing to tables in streaming queries is supported **only in UPSERT mode**. The `INSERT INTO` operation is not supported because, during reprocessing of events (the at-least-once guarantee), it would lead to duplicate rows. With `UPSERT`, if a row with the same primary key already exists, it will be updated; otherwise, a new row will be inserted, while `INSERT INTO` will fail with an error.

{% endnote %}


```yql
CREATE STREAMING QUERY write_table_example AS
DO BEGIN

-- Writing to a table (only UPSERT, INSERT not supported)
UPSERT INTO output_table
SELECT
    Id,
    Name
FROM
    ext_source.input_topic -- or local topic input_topic
WITH (
    FORMAT = json_each_row,  -- Input data format
    SCHEMA = (               -- Input data schema
        Id Uint64 NOT NULL,
        Name Utf8 NOT NULL
    )
);

END DO
```


For more details: [{#T}](table-writing.md).

## See also

- [Local and external topics](../../concepts/query_execution/topics.md#local-external-topics)
- [{#T}](../../yql/reference/syntax/create-streaming-query.md)
- [{#T}](../../recipes/streaming_queries/topics.md)
