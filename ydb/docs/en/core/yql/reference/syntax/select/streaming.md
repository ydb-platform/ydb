# Streaming read from a topic

You can read data from a [topic](../../../../concepts/datamodel/topic.md) using a regular `SELECT` without creating a [streaming query](../../../../concepts/streaming-query/streaming-query.md). To do this, specify `STREAMING = TRUE` in the `WITH` block and set a limit on the number of output rows via `LIMIT`; otherwise, the query will not complete.

{% note warning %}

This method is intended only for debugging and checking data in a topic. For production processes, create streaming queries using [CREATE STREAMING QUERY](../create-streaming-query.md).

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

* [{#T}](../../../../recipes/streaming_queries/debug-read.md) — recipe with additional examples
* [{#T}](../../../../concepts/streaming-query/streaming-query.md)
* [{#T}](../create-streaming-query.md)
