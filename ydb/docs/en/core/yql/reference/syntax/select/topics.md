# Reading data from a topic

You can read data from a [topic](../../../../concepts/datamodel/topic.md) using a regular `SELECT` without creating a streaming query.

{% note warning %}

This method is intended only for debugging and verifying data in a topic. For production processes, create streaming queries using [CREATE STREAMING QUERY](../create-streaming-query.md).

{% endnote %}

{% note info %}

In the examples:

- `ext_source` — a pre-created [`external data source`](../../../../concepts/datamodel/external_data_source.md)
- `input_topic` — a local or external topic.

For more information, see [local and external topics](../../../../concepts/query_execution/topics.md#local-external-topics).

{% endnote %}

## Examples

### Reading current data


```yql
SELECT
    Data
FROM
    ext_source.input_topic -- or local topic input_topic
WITH (
    FORMAT = raw,
    SCHEMA = (
        Data String
    )
)
```


### Reading with waiting for new data


```yql
SELECT
    Data
FROM
    input_topic -- or external topic ext_source.input_topic
WITH (
    FORMAT = raw,
    SCHEMA = (
        Data String
    ),
    STREAMING = "TRUE"
)
LIMIT 1
```


For more information, see [reading from a topic](../../../../concepts/query_execution/topics.md#topic-read).

## See also

* [{#T}](../../../../concepts/query_execution/topics.md#topic-read)
* streaming-query
* [{#T}](../create-streaming-query.md)
