# CREATE STREAMING QUERY

`CREATE STREAMING QUERY` creates a [streaming query](../../../concepts/streaming-query/streaming-query.md).

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

* `OR REPLACE` — if a streaming query with this name already exists, it will be replaced with a new query while preserving the read offsets from the topic.
* `IF NOT EXISTS` — do not output an error if a streaming query with this name already exists; in this case, the existing query remains unchanged.
* `query_name` — the name of the streaming query to create.
* `WITH (<key> = <value>)` — a list of settings for the new streaming query, optional.
* `AS DO BEGIN ... END DO` — the full text of the new streaming query, including all required SQL statements. Restrictions for the query text are given in [{#T}](../../../concepts/streaming-query/streaming-query.md#limitations), examples of the text [see below](#examples).

The `OR REPLACE` and `IF NOT EXISTS` settings cannot be used simultaneously.

Available parameters of the `WITH` block:

* `RUN = (TRUE|FALSE)` — start the query after creation, default `TRUE`.
* `RESOURCE_POOL = <resource_pool_name>` — name of the [resource pool](../../../concepts/glossary.md#resource-pool) in which the query will run.

Examples of creating a streaming query [see below](#examples).

## Using a consumer {#consumer-usage}

A [consumer](../../../concepts/datamodel/topic.md#consumer) is a named subscription to a [topic](../../../concepts/datamodel/topic.md) that stores the current read position.

A consumer is created via the [CLI](../../../reference/ydb-cli/topic-consumer-add.md) or when creating a topic using [CREATE TOPIC](create-topic.md). The consumer name is specified in the query text using a pragma:


```sql
PRAGMA pq.Consumer="my_consumer";
```


If no consumer is specified, reading from the topic is performed without a consumer. The read position in both cases is saved in a [checkpoint](../../../dev/streaming-query/checkpoints.md). Specifying a consumer allows tracking the read position and lag from the topic side, for example, via the [CLI](../../../reference/ydb-cli/topic-read.md).

## Examples {#examples}

### Writing to a topic (JSON) {#example-topic-json}

The query reads events from the input topic, forms a JSON object from individual fields, and writes the result to the output topic.

The `AsStruct` function creates a structure from the specified fields, `Yson::From` converts it to Yson, `Yson::SerializeJson` serializes it to a JSON string, and `ToBytes` converts it to the `String` type, which is required for writing to the topic.

{% note info %}

Streaming queries can work with [local and external topics](../../../dev/streaming-query/local-and-external-topics.md).

In the example:

- `ext_source` is a pre-created [`external data source`](../../../concepts/datamodel/external_data_source.md);
- `input_topic` and `output_topic` are local or external [topics](../../../concepts/datamodel/topic.md).

{% endnote %}


```yql
CREATE STREAMING QUERY my_streaming_query AS
DO BEGIN

    INSERT INTO output_topic -- or external topic ext_source.output_topic
    SELECT
        -- Forming JSON from individual fields
        ToBytes(Unwrap(Yson::SerializeJson(Yson::From(
            AsStruct(Id AS id, Name AS name)
        ))))
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


### Writing to a table {#example-table}

The query reads events from a topic and writes them to the `output_table` table. The table must be created in advance with a schema matching the selected columns.

{% note warning %}

Writing to tables in streaming queries is supported **only in UPSERT mode**. The `INSERT INTO` operation is not supported because, during event reprocessing (the [at-least-once](../../../concepts/streaming-query/streaming-query.md#guarantees) guarantee), it would lead to duplicate rows. With `UPSERT`, if a row with that primary key already exists, it will be updated; otherwise, a new row will be inserted, while `INSERT INTO` will fail with an error.

{% endnote %}


```sql
CREATE STREAMING QUERY my_streaming_query AS
DO BEGIN

    -- Writing to a table (only UPSERT, INSERT not supported)
    UPSERT INTO output_table
    SELECT
        Id,
        Name
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


### Running in a resource pool {#example-resource-pool}

The query is created in the specified [resource pool](../../../concepts/glossary.md#resource-pool) but is not started automatically (`RUN = FALSE`). This allows checking the configuration before starting or starting the query later via [ALTER STREAMING QUERY](alter-streaming-query.md).


```sql
CREATE STREAMING QUERY my_streaming_query WITH (
    RUN = FALSE,                      -- Do not start automatically
    RESOURCE_POOL = my_resource_pool  -- Resource pool for execution
) AS
DO BEGIN

    INSERT INTO output_topic -- or external topic ext_source.output_topic
    SELECT
        ToBytes(Unwrap(Yson::SerializeJson(Yson::From(
            AsStruct(Id AS id, Name AS name)
        ))))
    FROM
        ext_source.input_topic -- or local topic input_topic
    WITH (
        FORMAT = json_each_row,
        SCHEMA = (
            Id Uint64 NOT NULL,
            Name Utf8 NOT NULL
        )
    );

END DO
```


Other examples: [{#T}](../../../dev/streaming-query/patterns.md).

## See also

* [{#T}](../../../dev/streaming-query/patterns.md)
* [{#T}](../../../concepts/streaming-query/streaming-query.md)
* [{#T}](alter-streaming-query.md)
* [{#T}](drop-streaming-query.md)
