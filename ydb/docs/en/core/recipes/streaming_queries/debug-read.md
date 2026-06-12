# Debug reads from a topic

When developing [streaming queries](../../concepts/streaming-query.md), it is often useful to inspect what arrives in a [topic](../../concepts/datamodel/topic.md) without creating a full streaming query. Run a regular `SELECT` with `STREAMING = TRUE`.

{% note warning %}

For debugging and inspection only. For production, create streaming queries with [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md).

{% endnote %}

{% note info %}

In the examples, `ydb_source` is a pre-created [external data source](../../concepts/datamodel/external_data_source.md), and `topic_name` / `input_topic` are topics available through it.

{% endnote %}

## Raw reads

Simplest option — read messages in `raw` format without parsing:

```sql
SELECT
    Data
FROM
    ydb_source.topic_name
WITH (
    FORMAT = raw,
    SCHEMA = (
        Data String
    ),
    STREAMING = TRUE
)
LIMIT 1
```

`LIMIT` is required; without it the query never completes because it waits for new messages indefinitely.

## JSON parsing

If the topic stores JSON, parse fields directly:

```sql
SELECT
    *
FROM
    ydb_source.topic_name
WITH (
    FORMAT = json_each_row,
    SCHEMA = (
        Time String NOT NULL,
        Level String NOT NULL,
        Host String NOT NULL
    ),
    STREAMING = TRUE
)
LIMIT 5
```

## See also

* [{#T}](../../concepts/streaming-query.md)
* [{#T}](../../dev/streaming-query/streaming-query-formats.md) — supported data formats
* [{#T}](../../yql/reference/syntax/select/streaming.md) — `STREAMING = TRUE` in the YQL reference
