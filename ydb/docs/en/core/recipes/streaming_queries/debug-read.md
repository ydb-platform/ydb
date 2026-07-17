# Debug reading from a topic

When developing [streaming queries](../../concepts/streaming-query/streaming-query.md), it can be useful to quickly see what data is coming into a [topic](../../concepts/datamodel/topic.md) without creating a full streaming query. To do this, you can run a regular `SELECT` with the `STREAMING = TRUE` parameter.

{% note warning %}

This method is intended only for debugging and checking data in a topic. For production use, create streaming queries via [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md).

{% endnote %}

{% note info %}

In the examples:

- `ext_source` — a pre-created [external data source](../../concepts/datamodel/external_data_source.md);
- `input_topic` — a local or external topic (see [local and external topics in streaming queries](../../dev/streaming-query/local-and-external-topics.md)).

{% endnote %}

## Reading raw data

The simplest way is to read messages in `raw` format, without parsing the schema:


```sql
SELECT
    Data
FROM
    input_topic -- or external topic ext_source.input_topic
WITH (
    FORMAT = raw,
    SCHEMA = (
        Data String
    ),
    STREAMING = TRUE
)
LIMIT 1
```


The `LIMIT` parameter is required — without it, the query will not complete, as it will wait for new messages indefinitely.

## Reading with JSON parsing

If the data in the topic is stored in JSON format, you can parse it by fields immediately:


```sql
SELECT
    *
FROM
    input_topic -- or external topic ext_source.input_topic
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

* [{#T}](../../concepts/streaming-query/streaming-query.md)
* [{#T}](../../dev/streaming-query/streaming-query-formats.md) — supported data formats
* [{#T}](../../yql/reference/syntax/select/streaming.md) — description of `STREAMING = TRUE` in the YQL reference
