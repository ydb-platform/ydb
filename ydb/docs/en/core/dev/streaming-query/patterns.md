# Common streaming query patterns

<<<<<<< HEAD
This section collects minimal examples of [streaming queries](../../concepts/streaming-query.md) for typical scenarios. It starts with a basic topic read, then shows end-to-end processing: handling data and writing results to a topic as JSON, to a topic as a plain string, and to a table. Each example can be used as a starting point for your own workloads.

## Reading from a topic {#topic-read}

Read from a topic using `SELECT ... FROM ... WITH (FORMAT, SCHEMA)`. The `WITH` block specifies the input format and schema—the fields expected in each message and their types. This pattern appears in all examples below.

{% note info %}

Topics are accessed through an [external data source](../../concepts/datamodel/external_data_source.md).

In the examples:
=======
This section collects minimal examples of [streaming queries](../../concepts/streaming-query/streaming-query.md) for typical scenarios. It starts with a basic topic read, then shows end-to-end processing: handling data and writing results to a topic as JSON, to a topic as a plain string, and to a table. Each example can be used as a starting point for your own workloads.

## ⟦C1⟧ — topic to read from; {#topic-read}

Data is read from a topic using `SELECT ... FROM ... WITH (FORMAT, SCHEMA)`. The `WITH` block specifies the input data format and schema — which fields are expected in each message and their types. This pattern is used in all subsequent examples.

{% note info %}

Working with [local and external topics](local-and-external-topics.md) is shown.

In the examples:

- `ext_source` — a pre-created `external data source`.
- `input_topic` — the topic from which data is read.
- `output_topic` — the topic where results are written.
- `output_table` — the {{ ydb-short-name }} table where results are written.

{% endnote %}

The following fragment shows reading events from a topic in JSON format. It is used inside [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md) in the `DO BEGIN ... END DO` block:
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

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
<<<<<<< HEAD
    ydb_source.input_topic
=======
    ext_source.input_topic -- or local topic input_topic
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))
WITH (
    FORMAT = json_each_row,
    SCHEMA = (
        Id Uint64 NOT NULL,
        Name Utf8 NOT NULL
    )
);
```

<<<<<<< HEAD
For more on formats, see [{#T}](streaming-query-formats.md).
=======

The following snippet reads JSON events from a topic. Use it inside [CREATE STREAMING QUERY](streaming-query-formats.md) in a ⟦C1⟧ block:
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

## Writing to a topic (JSON) {#topic-json}

The query reads events from the input topic, builds a JSON object from fields, and writes to the output topic. `AsStruct` builds a structure from the fields, `Yson::From` converts it to Yson, `Yson::SerializeJson` serializes to a JSON string, and `ToBytes` converts to `String`, which is required for topic writes.
<<<<<<< HEAD
=======

>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

```yql
CREATE STREAMING QUERY write_json_example AS
DO BEGIN

-- ydb_source — external data source for topics
INSERT INTO ydb_source.output_topic
SELECT
<<<<<<< HEAD
    -- Build JSON from fields
=======
    -- Forming JSON from individual fields
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))
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

<<<<<<< HEAD
=======

>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))
More on the functions:

- [AsStruct](../../yql/reference/builtins/basic#as-container)
- [Yson::From](../../yql/reference/udf/list/yson#ysonfrom)
- [Yson::SerializeJson](../../yql/reference/udf/list/yson#ysonserializejson)
- [Unwrap](../../yql/reference/builtins/basic#unwrap)
- [ToBytes](../../yql/reference/builtins/basic#to-from-bytes).

## Writing to a topic (string) {#topic-utf8}

The query reads events from the input topic and writes a single field as a string to the output topic. Topic writes require `SELECT` to return a single column of type `String` or `Utf8`.
<<<<<<< HEAD
=======

>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

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

<<<<<<< HEAD
=======

>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))
More on write formats: [{#T}](streaming-query-formats.md#write_formats).

## Writing to a table {#table-write}

The query reads events from a topic and writes them to `output_table`. Create the table beforehand with a schema that matches the selected columns.

{% note warning %}

<<<<<<< HEAD
Table writes in streaming queries support **UPSERT only**. `INSERT INTO` is not supported: with [at-least-once](../../concepts/streaming-query.md#guarantees) delivery, retries would duplicate rows. With `UPSERT`, an existing row with the same primary key is updated; otherwise a new row is inserted, while `INSERT INTO` fails.
=======
Table writes in streaming queries support **UPSERT only**. `INSERT INTO` is not supported: with [at-least-once](../../concepts/streaming-query/streaming-query.md#guarantees) delivery, retries would duplicate rows. With `UPSERT`, an existing row with the same primary key is updated; otherwise a new row is inserted, while `INSERT INTO` fails.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

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

<<<<<<< HEAD
=======

>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))
More details: [{#T}](table-writing.md).

## See also

<<<<<<< HEAD
=======
- [Local and external topics in streaming queries](local-and-external-topics.md)
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))
- [{#T}](../../yql/reference/syntax/create-streaming-query.md)
- [{#T}](../../recipes/streaming_queries/topics.md)
