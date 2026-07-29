# Debug reads from a topic

<<<<<<< HEAD
When developing [streaming queries](../../concepts/streaming-query.md), it is often useful to inspect what arrives in a [topic](../../concepts/datamodel/topic.md) without creating a full streaming query. Run a regular `SELECT` with `STREAMING = TRUE`.

{% note warning %}

For debugging and inspection only. For production, create streaming queries with [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md).
=======
When developing [streaming queries](../../concepts/streaming-query/streaming-query.md), it is useful to quickly see what data is coming into a [topic](../../concepts/datamodel/topic.md) without creating a full streaming query. To do this, you can run a regular `SELECT` with the `STREAMING = TRUE` parameter.

{% note warning %}

This method is intended only for debugging and checking data in a topic. For production use, create streaming queries using [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

{% endnote %}

{% note info %}

<<<<<<< HEAD
In the examples, `ydb_source` is a pre-created [external data source](../../concepts/datamodel/external_data_source.md), and `topic_name` / `input_topic` are topics available through it.

{% endnote %}

## Raw reads

Simplest option — read messages in `raw` format without parsing:
=======
In the examples:

- `ext_source` — a pre-created [external data source](../../concepts/datamodel/external_data_source.md).
- `input_topic` — a local or external topic (see [local and external topics in streaming queries](../../dev/streaming-query/local-and-external-topics.md)).

{% endnote %}

## Reading raw data

The simplest way is to read messages in `raw` format, without parsing the schema:

>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

```sql
SELECT
    Data
FROM
<<<<<<< HEAD
    ydb_source.topic_name
=======
    input_topic -- or external topic ext_source.input_topic
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))
WITH (
    FORMAT = raw,
    SCHEMA = (
        Data String
    ),
    STREAMING = TRUE
)
LIMIT 1
```

<<<<<<< HEAD
`LIMIT` is required; without it the query never completes because it waits for new messages indefinitely.

## JSON parsing

If the topic stores JSON, parse fields directly:
=======

The `LIMIT` parameter is required — without it, the query will not complete, as it will wait for new messages indefinitely.

## Reading with JSON parsing

If the data in the topic is stored in JSON format, you can immediately parse it by fields:

>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

```sql
SELECT
    *
FROM
<<<<<<< HEAD
    ydb_source.topic_name
=======
    input_topic -- or external topic ext_source.input_topic
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))
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

<<<<<<< HEAD
## See also

* [{#T}](../../concepts/streaming-query.md)
* [{#T}](../../dev/streaming-query/streaming-query-formats.md) — supported data formats
* [{#T}](../../yql/reference/syntax/select/streaming.md) — `STREAMING = TRUE` in the YQL reference
=======

## See also

* [{#T}](../../concepts/streaming-query/streaming-query.md)
* [{#T}](../../dev/streaming-query/streaming-query-formats.md) — supported data formats
* [{#T}](../../yql/reference/syntax/select/streaming.md) — description of `STREAMING = TRUE` in the YQL reference
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))
