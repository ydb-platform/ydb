# Streaming data reads from a topic

You can read data from a [topic](../../../../concepts/datamodel/topic.md) using a regular `SELECT` without creating a [streaming query](../../../../concepts/streaming-query/streaming-query.md). To do this, specify `STREAMING = TRUE` in the `WITH` clause and set a limit on the number of output rows using `LIMIT`; otherwise, the query will not complete.

{% note warning %}

This method is intended only for debugging and checking data in a topic. For production processes, create streaming queries using [CREATE STREAMING QUERY](../create-streaming-query.md).

{% endnote %}

{% note info %}

In the examples:

- `ext_source` is a pre-created [external data source](../../../../concepts/datamodel/external_data_source.md);
- `input_topic` is a local or external topic.

For more details, see [local and external topics in streaming queries](../../../../dev/streaming-query/local-and-external-topics.md).

{% endnote %}

## Example


```yql
SELECT
    Data
FROM
    ext_source.input_topic -- or local topic input_topic
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

* [{#T}](../../../../recipes/streaming_queries/debug-read.md) — a recipe with additional examples
* [{#T}](../../../../concepts/streaming-query/streaming-query.md)
* [{#T}](../create-streaming-query.md)
