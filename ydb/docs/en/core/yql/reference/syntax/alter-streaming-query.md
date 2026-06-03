# ALTER STREAMING QUERY

`ALTER STREAMING QUERY` changes settings{% if alter_streaming_query == true %} and query text {% endif %} of [streaming queries](../../../concepts/streaming-query.md) and controls their lifecycle (start and stop).

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

* `IF EXISTS` — do not fail if the streaming query does not exist.
* `query_name` — name of the streaming query to change.
* `SET (<key> = <value>)` — optional list of settings to update.

{% if alter_streaming_query == true %}
* `AS DO BEGIN ... END DO` — optional new streaming query text.

You must specify a `SET` block, new query text, or both.

{% endif %}

### Changing query settings

Syntax:

```sql
ALTER STREAMING QUERY [IF EXISTS] <query_name> SET (<key> = <value>)
```

Available parameters:

* `RUN = (TRUE|FALSE)` — start or stop the query.
* `RESOURCE_POOL = <resource_pool_name>` — name of the [resource pool](../../../concepts/glossary.md#resource-pool) where the query runs.
{% if alter_streaming_query == true %}
* `FORCE = (TRUE|FALSE)` — allow changing query text with aggregation state reset. Required for [text changes](#text-changing-examples).
{% endif %}

When you run `SET (RUN = TRUE)`, read offsets from topics and aggregation state are restored from a [checkpoint](../../../dev/streaming-query/checkpoints.md). If there is no checkpoint, reading starts from the latest data.

Examples of changing settings are [below](#parameters-changing-examples).

{% if alter_streaming_query == true %}

### Changing query text

Syntax:

```sql
ALTER STREAMING QUERY [IF EXISTS] <query_name> AS
DO BEGIN
    <query_statement>
END DO
```

Where:

* `<query_statement>` — new streaming query text. Limitations are in [{#T}](../../../concepts/streaming-query.md#limitations); examples are [below](#text-changing-examples).

{% note warning %}

Changing query text while fully preserving the [checkpoint](../../../dev/streaming-query/checkpoints.md) is under development. `FORCE = TRUE` is required.

If aggregation state cannot be restored, the command fails with:

```text
Changing the query text will result in the loss of the checkpoint. Please use FORCE=true to change the request text
```

After a change with `FORCE = TRUE`, only read offsets from topics are restored; aggregation state is reset.

{% endnote %}

If `RUN` is not specified in `SET`, the query keeps its running state. A running query is restarted with the new text.

Examples of changing text are [below](#text-changing-examples).

{% endif %}

## Permissions

Streaming queries require [permission](./grant.md#permissions-list) `ALTER SCHEMA`. Example grant for `my_streaming_query`:

```sql
GRANT ALTER SCHEMA ON my_streaming_query TO `user@domain`
```

## Examples

### Changing settings {#parameters-changing-examples}

Stop `my_streaming_query`:

```sql
ALTER STREAMING QUERY my_streaming_query SET (
    RUN = FALSE
)
```

Start `my_streaming_query` in [resource pool](../../../concepts/glossary.md#resource-pool) `my_resource_pool`:

```sql
ALTER STREAMING QUERY my_streaming_query SET (
    RUN = TRUE,
    RESOURCE_POOL = my_resource_pool
)
```

{% if alter_streaming_query == true %}

### Changing query text {#text-changing-examples}

Change the text of `my_streaming_query` and reset aggregation state. After start, only read offsets from topics are restored from the [checkpoint](../../../dev/streaming-query/checkpoints.md):

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

Current status is available in the `Status` column of the `.sys/streaming_queries` system view [{#T}](../../../dev/system-views.md#streaming_queries):

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

1. `CREATING` — query is being created after `CREATE STREAMING QUERY`.
2. `CREATED` — query exists but is not running (for example, `RUN = FALSE`).
3. `STARTING` — query is starting.
4. `RUNNING` — query is executing.
5. `SUSPENDED` — query paused due to internal errors; the system will retry.
6. `STOPPING` — query is stopping after `ALTER STREAMING QUERY ... SET (RUN = FALSE)`.
7. `STOPPED` — query is stopped.

After successful DDL for create or alter, status is guaranteed to be `CREATED`, `STARTING`, `RUNNING`, `STOPPED`, or `SUSPENDED` depending on `RUN = (TRUE|FALSE)` and whether startup succeeded.

More examples for other data formats: [{#T}](../../../dev/streaming-query/streaming-query-formats.md). For capabilities and limitations of streaming queries, see [{#T}](../../../concepts/streaming-query.md).

## See also

* [{#T}](../../../concepts/streaming-query.md)
* [{#T}](create-streaming-query.md)
* [{#T}](drop-streaming-query.md)
