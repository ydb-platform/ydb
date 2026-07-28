# Debug reading from a topic

When developing streaming queries, it is useful to quickly see what data is coming into a [topic](../../concepts/datamodel/topic.md) without creating a full streaming query. To do this, run a regular `SELECT` from the topic.

For a detailed description of reading from a topic, see [{#T}](../../concepts/query_execution/topics.md).

{% note warning %}

Reading via `SELECT` is intended for debugging only. For production use, create streaming queries using [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md).

{% endnote %}

## See also

* [{#T}](../../concepts/query_execution/topics.md)
* [{#T}](../../yql/reference/syntax/select/topics.md)
* [{#T}](../../dev/streaming-query/streaming-query-formats.md)
