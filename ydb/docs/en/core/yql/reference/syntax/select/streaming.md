# Streaming reads from a topic

You can read from a [topic](../../../../concepts/datamodel/topic.md) with a regular `SELECT` without creating a [streaming query](../../../../concepts/streaming-query.md). Add `STREAMING = TRUE` in the `WITH` block and limit rows with `LIMIT`; otherwise the query never completes.

{% note warning %}

For debugging only. For production workloads, use [CREATE STREAMING QUERY](../create-streaming-query.md).

{% endnote %}

{% note info %}

In the examples, `ydb_source` is a pre-created [external data source](../../../../concepts/datamodel/external_data_source.md), and `topic_name` is a topic available through it.

{% endnote %}

## Example

```yql
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

## See also

* [{#T}](../../../../recipes/streaming_queries/debug-read.md) — recipe with more examples
* [{#T}](../../../../concepts/streaming-query.md)
* [{#T}](../create-streaming-query.md)
