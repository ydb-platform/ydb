# CREATE STREAMING QUERY

`CREATE STREAMING QUERY` creates a streaming query.

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
* `IF NOT EXISTS` — do not output an error if a streaming query with this name already exists; in this case, the existing query will remain unchanged.
* `query_name` — the name of the streaming query to create.
* `WITH (<key> = <value>)` — a list of settings for the new streaming query, optional.
* `AS DO BEGIN ... END DO` — the full text of the new streaming query, including all required SQL statements. Restrictions on the query text are given in {#T}, see [below](#examples) for query text examples.

The `OR REPLACE` and `IF NOT EXISTS` settings cannot be used simultaneously.

Available parameters of the `WITH` block:

* `RUN = (TRUE|FALSE)` — start the query after creation, default `TRUE`.
* `RESOURCE_POOL = <resource_pool_name>` — the name of the [resource pool](../../../concepts/glossary.md#resource-pool) in which the query will run.

See [below](#examples) for examples of creating a streaming query.

## Using a reader {#consumer-usage}

{% include [consumer-usage](../../../_includes/consumer-usage.md) %}

Regardless of whether a reader exists, the read position of the streaming query is saved in a [checkpoint](../../../dev/streaming-query/checkpoints.md).

## Examples {#examples}

### Writing to a topic (JSON) {#example-topic-json}

The query reads events from an input topic, forms a JSON object from individual fields, and writes the result to an output topic.

The `AsStruct` function creates a structure from the specified fields, `Yson::From` converts it to Yson, `Yson::SerializeJson` serializes it to a JSON string, and `ToBytes` converts it to the `String` type, which is required for writing to a topic.

{% note info %}

Streaming queries can work with [local and external topics](../../../concepts/query_execution/topics.md#local-external-topics).

In the example:

- `ext_source` is a pre-created [`external data source`](../../../concepts/datamodel/external_data_source.md).
- `input_topic` and `output_topic` are local or external [topics](../../../concepts/datamodel/topic.md).

{% endnote %}


```yql
CREATE STREAMING QUERY my_streaming_query AS
DO BEGIN

    INSERT INTO output_topic -- or external topic ext_source.output_topic
    SELECT
        -- Formation of JSON from individual fields
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

The query reads events from the topic and writes them to the `output_table` table. The table must be created in advance with a schema matching the selected columns.

{% note warning %}

Writing to tables in streaming queries is supported **only in UPSERT mode**. The `INSERT INTO` operation is not supported, because during reprocessing of events (the at-least-once guarantee) it would lead to duplicate rows. With `UPSERT`, if a row with such a primary key already exists, it will be updated; otherwise, a new row will be inserted, and `INSERT INTO` will fail with an error.

{% endnote %}


```sql
CREATE STREAMING QUERY my_streaming_query AS
DO BEGIN

    -- Write to table (only UPSERT, INSERT is not supported)
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

The query is created in the specified [resource pool](../../../concepts/glossary.md#resource-pool) but is not started automatically (`RUN = FALSE`). This allows you to check the configuration before starting or start the query later via [ALTER STREAMING QUERY](alter-streaming-query.md).


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
* {#T}
* [{#T}](alter-streaming-query.md)
* [{#T}](drop-streaming-query.md)
