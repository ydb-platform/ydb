# CREATE STREAMING QUERY

<<<<<<< HEAD
`CREATE STREAMING QUERY` creates a [streaming query](../../../concepts/streaming-query.md).
=======
`CREATE STREAMING QUERY` creates a [streaming query](../../../concepts/streaming-query/streaming-query.md).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

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

<<<<<<< HEAD
* `OR REPLACE` — if a streaming query with this name already exists, replace it while preserving read offsets from topics.
* `IF NOT EXISTS` — do not fail if a streaming query with this name already exists; leave the existing query unchanged.
* `query_name` — name of the streaming query to create.
* `WITH (<key> = <value>)` — optional list of settings for the new streaming query.
* `AS DO BEGIN ... END DO` — full query text including all SQL statements. Limitations are described in [{#T}](../../../concepts/streaming-query.md#limitations); examples are [below](#examples).

You cannot use `OR REPLACE` and `IF NOT EXISTS` together.
=======
* `OR REPLACE` — if a streaming query with this name already exists, it will be replaced with a new query while preserving the read offsets from the topic.
* `IF NOT EXISTS` — do not output an error if a streaming query with this name already exists; in this case, the existing query will remain unchanged.
* `query_name` — the name of the streaming query to create.
* `WITH (<key> = <value>)` — a list of settings for the new streaming query, optional.
* `AS DO BEGIN ... END DO` — the full text of the new streaming query, including all necessary SQL statements. Restrictions for the query text are given in [{#T}](../../../concepts/streaming-query/streaming-query.md#limitations), see [below](#examples) for query text examples.

Settings `OR REPLACE` and `IF NOT EXISTS` cannot be used simultaneously.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

`WITH` parameters:

* `RUN = (TRUE|FALSE)` — start the query after creation; default `TRUE`.
* `RESOURCE_POOL = <resource_pool_name>` — name of the [resource pool](../../../concepts/glossary.md#resource-pool) where the query runs.

Creation examples are [below](#examples).

<<<<<<< HEAD
## Consumer usage {#consumer-usage}

A [consumer](../../../concepts/datamodel/topic.md#consumer) is a named subscription to a [topic](../../../concepts/datamodel/topic.md) that stores the current read position.

Create a consumer with the [CLI](../../../reference/ydb-cli/topic-consumer-add.md) or when creating a topic with [CREATE TOPIC](create-topic.md). Set the consumer name in the query with a pragma:
=======
## Using a Consumer {#consumer-usage}

A [consumer](../../../concepts/datamodel/topic.md#consumer) is a named subscription to a [topic](../../../concepts/datamodel/topic.md) that stores the current read position.

A consumer is created via the [CLI](../../../reference/ydb-cli/topic-consumer-add.md) or when creating a topic using [CREATE TOPIC](create-topic.md). The consumer name is specified in the query text using a pragma:

>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

```sql
PRAGMA pq.Consumer="my_consumer";
```

<<<<<<< HEAD
If no consumer is specified, the topic is read without a named consumer. In both cases the read position is stored in a [checkpoint](../../../dev/streaming-query/checkpoints.md). A consumer lets you track position and lag from the topic side, for example via the [CLI](../../../reference/ydb-cli/topic-read.md).

## Examples {#examples}

### Write to a topic (JSON) {#example-topic-json}
=======

If no consumer is specified, reading from the topic is performed without a consumer. In both cases, the read position is saved in a [checkpoint](../../../dev/streaming-query/checkpoints.md). Specifying a consumer allows tracking the read position and lag from the topic side, for example, via the [CLI](../../../reference/ydb-cli/topic-read.md).

## Examples {#examples}

### Writing to a Topic (JSON) {#example-topic-json}
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

The query reads events from the input topic, builds a JSON object from fields, and writes to the output topic.

`AsStruct` builds a structure, `Yson::From` converts to Yson, `Yson::SerializeJson` serializes to JSON, and `ToBytes` converts to `String` for topic writes.

{% note info %}

<<<<<<< HEAD
Topic writes go through an [external data source](../../../concepts/datamodel/external_data_source.md). In the example, `ydb_source` is a pre-created external data source; `output_topic` and `input_topic` are topics available through it.
=======
Streaming queries can work with [local and external topics](../../../dev/streaming-query/local-and-external-topics.md).

In the example:

- `ext_source` is a pre-created [`external data source`](../../../concepts/datamodel/external_data_source.md).
- `input_topic` and `output_topic` are local or external [topics](../../../concepts/datamodel/topic.md).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

{% endnote %}

```yql
CREATE STREAMING QUERY my_streaming_query AS
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

<<<<<<< HEAD
The query reads events from a topic and writes them to `output_table`. Create the table beforehand with a matching schema.

{% note warning %}

Table writes in streaming queries support **UPSERT only**. `INSERT INTO` is not supported: with [at-least-once](../../../concepts/streaming-query.md#guarantees) retries, it would duplicate rows. With `UPSERT`, an existing row with the same primary key is updated; otherwise a row is inserted, while `INSERT INTO` fails.
=======
### Writing to a Table {#example-table}

The query reads events from a topic and writes them to the `output_table` table. The table must be created in advance with a schema matching the selected columns.

{% note warning %}

Writing to tables in streaming queries is supported **only in UPSERT mode**. The `INSERT INTO` operation is not supported because, during reprocessing of events (at-least-once guarantee [at-least-once](../../../concepts/streaming-query/streaming-query.md#guarantees)), it would lead to duplicate rows. With `UPSERT`, if a row with the same primary key already exists, it will be updated; otherwise, a new row will be inserted, and `INSERT INTO` will fail with an error.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

{% endnote %}

```sql
CREATE STREAMING QUERY my_streaming_query AS
DO BEGIN

<<<<<<< HEAD
    -- Table write (UPSERT only; INSERT not supported)
=======
    -- Writing to table (only UPSERT, INSERT not supported)
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))
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

<<<<<<< HEAD
The query is created in the given [resource pool](../../../concepts/glossary.md#resource-pool) but not started automatically (`RUN = FALSE`). You can validate configuration first or start later with [ALTER STREAMING QUERY](alter-streaming-query.md).
=======
### Running in a Resource Pool {#example-resource-pool}

The query is created in the specified [resource pool](../../../concepts/glossary.md#resource-pool) but is not started automatically (`RUN = FALSE`). This allows you to check the configuration before starting or start the query later via [ALTER STREAMING QUERY](alter-streaming-query.md).

>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

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
<<<<<<< HEAD
* [{#T}](../../../concepts/streaming-query.md)
=======
* [{#T}](../../../concepts/streaming-query/streaming-query.md)
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))
* [{#T}](alter-streaming-query.md)
* [{#T}](drop-streaming-query.md)
