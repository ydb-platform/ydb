A [consumer](../concepts/datamodel/topic.md#consumer) is a named subscription to a [topic](../concepts/datamodel/topic.md) that stores the current read position.

A consumer is created via the [CLI](../reference/ydb-cli/topic-consumer-add.md) or when creating a topic using [CREATE TOPIC](../yql/reference/syntax/create-topic.md). The consumer name is specified in the query text with a pragma:


```sql
PRAGMA pq.Consumer="my_consumer";
```


If a consumer is not specified, reading from the topic is performed without one. Specifying a consumer allows tracking the read position and lag from the topic side, for example via the [CLI](../reference/ydb-cli/topic-read.md).
