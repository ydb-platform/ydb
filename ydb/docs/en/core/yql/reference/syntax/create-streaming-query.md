# CREATE STREAMING QUERY

`CREATE STREAMING QUERY` creates a [streaming query](../../../concepts/streaming-query.md).

## Syntax

```sql
CREATE [OR REPLACE] STREAMING QUERY [IF NOT EXISTS] <query_name> [WITH (
    <key1> = <value1>,
    <key2> = <value2>,
    ...
)] AS
DO BEGIN
    <query_statement1>;
    <query_statement2>;
    ...
END DO
```

### Parameters

* `OR REPLACE` — if a streaming query with this name already exists, replace it while preserving read offsets from topics.
* `IF NOT EXISTS` — do not fail if a streaming query with this name already exists; leave the existing query unchanged.
* `query_name` — name of the streaming query to create.
* `WITH (<key> = <value>)` — optional list of settings for the new streaming query.
* `AS DO BEGIN ... END DO` — full query text including all SQL statements. Limitations are described in [{#T}](../../../concepts/streaming-query.md#limitations); examples are [below](#examples).

You cannot use `OR REPLACE` and `IF NOT EXISTS` together.

`WITH` parameters:

* `RUN = (TRUE|FALSE)` — start the query after creation; default `TRUE`.
* `RESOURCE_POOL = <resource_pool_name>` — name of the [resource pool](../../../concepts/glossary.md#resource-pool) where the query runs.

Creation examples are [below](#examples).

## Consumer usage {#consumer-usage}

A [consumer](../../../concepts/datamodel/topic.md#consumer) is a named subscription to a [topic](../../../concepts/datamodel/topic.md) that stores the current read position.

Create a consumer with the [CLI](../../../reference/ydb-cli/topic-consumer-add.md) or when creating a topic with [CREATE TOPIC](create-topic.md). Set the consumer name in the query with a pragma:

```sql
PRAGMA pq.Consumer="my_consumer";
```

If no consumer is specified, the topic is read without a named consumer. In both cases the read position is stored in a [checkpoint](../../../dev/streaming-query/checkpoints.md). A consumer lets you track position and lag from the topic side, for example via the [CLI](../../../reference/ydb-cli/topic-read.md).

## Examples {#examples}

### Write to a topic (JSON) {#example-topic-json}

The query reads events from the input topic, builds a JSON object from fields, and writes to the output topic.

`AsStruct` builds a structure, `Yson::From` converts to Yson, `Yson::SerializeJson` serializes to JSON, and `ToBytes` converts to `String` for topic writes.

{% note info %}

Topic writes go through an [external data source](../../../concepts/datamodel/external_data_source.md). In the example, `ydb_source` is a pre-created external data source; `output_topic` and `input_topic` are topics available through it.

{% endnote %}

```yql
CREATE STREAMING QUERY my_streaming_query AS
DO BEGIN

    -- ydb_source — external data source for topics
    INSERT INTO ydb_source.output_topic
    SELECT
        -- Build JSON from fields
        ToBytes(Unwrap(Yson::SerializeJson(Yson::From(
            AsStruct(Id AS id, Name AS name)
        ))))
    FROM
        -- Read from topic
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

### Write to a table {#example-table}

The query reads events from a topic and writes them to `output_table`. Create the table beforehand with a matching schema.

{% note warning %}

Table writes in streaming queries support **UPSERT only**. `INSERT INTO` is not supported: with [at-least-once](../../../concepts/streaming-query.md#guarantees) retries, it would duplicate rows. With `UPSERT`, an existing row with the same primary key is updated; otherwise a row is inserted, while `INSERT INTO` fails.

{% endnote %}

```sql
CREATE STREAMING QUERY my_streaming_query AS
DO BEGIN

    -- Table write (UPSERT only; INSERT not supported)
    UPSERT INTO output_table
    SELECT
        Id,
        Name
    FROM
        -- ydb_source — external data source for topics
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

### Start in a resource pool {#example-resource-pool}

The query is created in the given [resource pool](../../../concepts/glossary.md#resource-pool) but not started automatically (`RUN = FALSE`). You can validate configuration first or start later with [ALTER STREAMING QUERY](alter-streaming-query.md).

```sql
CREATE STREAMING QUERY my_streaming_query WITH (
    RUN = FALSE,                      -- Do not auto-start
    RESOURCE_POOL = my_resource_pool  -- Pool for execution
) AS
DO BEGIN

    -- ydb_source — external data source for topics
    INSERT INTO ydb_source.output_topic
    SELECT
        ToBytes(Unwrap(Yson::SerializeJson(Yson::From(
            AsStruct(Id AS id, Name AS name)
        ))))
    FROM
        ydb_source.input_topic
    WITH (
        FORMAT = json_each_row,
        SCHEMA = (
            Id Uint64 NOT NULL,
            Name Utf8 NOT NULL
        )
    );

END DO
```

More examples: [{#T}](../../../dev/streaming-query/patterns.md).

## See also

* [{#T}](../../../dev/streaming-query/patterns.md)
* [{#T}](../../../concepts/streaming-query.md)
* [{#T}](alter-streaming-query.md)
* [{#T}](drop-streaming-query.md)
