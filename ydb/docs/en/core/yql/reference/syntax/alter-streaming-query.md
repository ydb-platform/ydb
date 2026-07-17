# ALTER STREAMING QUERY

`ALTER STREAMING QUERY` changes settings{% if alter_streaming_query == true %} and text {% endif %} of [streaming queries](../../../concepts/streaming-query/streaming-query.md), and also manages their state: starting and stopping.

## Syntax

{% if alter_streaming_query == true %}

```sql
ALTER STREAMING QUERY [IF EXISTS] <query_name> [SET (
    <key1> = <value1>,
    <key2> = <value2>,
    ...
)] [AS
DO BEGIN
    <query_statement1>;
    <query_statement2>;
    ...
END DO]
```

{% else %}

```sql
ALTER STREAMING QUERY [IF EXISTS] <query_name> SET (
    <key1> = <value1>,
    <key2> = <value2>,
    ...
)
```

{% endif %}

### Parameters

* `IF EXISTS` — do not output an error if the streaming query does not exist.
* `query_name` — name of the streaming query to be modified.
* `SET (<key> = <value>)` — list of streaming query settings to update, optional.

{% if alter_streaming_query == true %}

* `AS DO BEGIN ... END DO` — new text of the streaming query, optional.

You must specify the `SET` block, the new query text, or both parameters simultaneously.

{% endif %}

### Changing query parameters

Syntax:


```sql
ALTER STREAMING QUERY [IF EXISTS] <query_name> SET (<key> = <value>)
```


Available parameters:

* `RUN = (TRUE|FALSE)` — start or stop the query.
* `RESOURCE_POOL = <resource_pool_name>` — name of the [resource pool](../../../concepts/glossary.md#resource-pool) in which the query will run.

{% if alter_streaming_query == true %}

* `FORCE = (TRUE|FALSE)` — allow changing the query text with reset of aggregation state. Required when [changing the query text](#text-changing-examples).

{% endif %}

When `SET (RUN = TRUE)` is executed, read offsets from the topic and aggregation function states are restored from the [checkpoint](../../../dev/streaming-query/checkpoints.md). If no checkpoint exists, reading starts from the most recent data.

See [below](#parameters-changing-examples) for examples of changing query parameters.

{% if alter_streaming_query == true %}

### Changing the query text

Syntax:


```sql
ALTER STREAMING QUERY [IF EXISTS] <query_name> AS
DO BEGIN
    <query_statement>
END DO
```


Where:

* `<query_statement>` — new text of the streaming query. Limitations are given in [{#T}](../../../concepts/streaming-query/streaming-query.md#limitations), examples — [below](#text-changing-examples).

{% note warning %}

Support for changing the query text with full [checkpoint](../../../dev/streaming-query/checkpoints.md) migration is under development. The `FORCE = TRUE` setting is mandatory.

If restoring aggregation function states is impossible, the command will fail with an error:


```text
Changing the query text will result in the loss of the checkpoint. Please use FORCE=true to change the request text
```


After changing with `FORCE = TRUE`, only read offsets from the topic are restored; aggregation states are reset.

{% endnote %}

If the `RUN` setting is not specified in the `SET` block, the query retains its state. A running query will be restarted with the new text.

See [below](#text-changing-examples) for examples of changing the query text.

{% endif %}

## Permissions

Working with streaming queries requires the [permission](./grant.md#permissions-list) `ALTER SCHEMA`. An example of granting such a permission for query `my_streaming_query`:


```sql
GRANT ALTER SCHEMA ON my_streaming_query TO `user@domain`
```


## Examples

### Changing parameters {#parameters-changing-examples}

Stopping query `my_streaming_query`:


```sql
ALTER STREAMING QUERY my_streaming_query SET (
    RUN = FALSE
)
```


Starting query `my_streaming_query` in the [resource pool](../../../concepts/glossary.md#resource-pool) `my_resource_pool`:


```sql
ALTER STREAMING QUERY my_streaming_query SET (
    RUN = TRUE,
    RESOURCE_POOL = my_resource_pool
)
```


{% if alter_streaming_query == true %}

### Changing the text {#text-changing-examples}

Changing the text of query `my_streaming_query` with reset of aggregation state. After starting, only read offsets from the topic are restored from the [checkpoint](../../../dev/streaming-query/checkpoints.md):


```sql
ALTER STREAMING QUERY my_streaming_query SET (
    FORCE = TRUE
) AS
DO BEGIN

INSERT INTO
    ydb_source.output_topic
SELECT
    *
FROM
    ydb_source.input_topic;

END DO
```

{% endif %}

### Query status {#status-of-query}

The current query status is available in the `Status` column of the [.sys/streaming_queries](../../../dev/system-views.md) system table:


```sql
SELECT
    Path,
    Status,
    Text,
    Run
FROM
    `.sys/streaming_queries`
```


Possible status values:

1. `CREATING` — the query is being created after executing the `CREATE STREAMING QUERY` command.
2. `CREATED` — the query is created but not started (for example, when `RUN = FALSE` is specified).
3. `STARTING` — the query is starting.
4. `RUNNING` — the query is running.
5. `SUSPENDED` — the query is paused due to internal errors. The system will automatically retry starting.
6. `STOPPING` — the query is stopping by the `ALTER STREAMING QUERY ... SET (RUN = FALSE)` command.
7. `STOPPED` — the query is stopped.

It is guaranteed that at the time of successful completion of a DDL for creating or modifying a streaming query, the status will be `CREATED`, `STARTING`, `RUNNING`, `STOPPED`, or `SUSPENDED` depending on the `RUN = (TRUE|FALSE)` setting and the success of starting the query.

Examples of processing data in other formats are given in the article [{#T}](../../../dev/streaming-query/streaming-query-formats.md). For more details on the capabilities and limitations of streaming queries, [see the documentation](../../../concepts/streaming-query/streaming-query.md).

## See also

* [{#T}](../../../concepts/streaming-query/streaming-query.md)
* [{#T}](create-streaming-query.md)
* [{#T}](drop-streaming-query.md)
