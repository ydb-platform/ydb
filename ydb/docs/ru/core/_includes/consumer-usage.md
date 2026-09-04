[Читатель (consumer)](../concepts/datamodel/topic.md#consumer) — именованная подписка на [топик](../concepts/datamodel/topic.md), которая хранит текущую позицию чтения.

Читатель создаётся через [CLI](../reference/ydb-cli/topic-consumer-add.md) или при создании топика с помощью [CREATE TOPIC](../yql/reference/syntax/create-topic.md). Имя читателя указывается в тексте запроса прагмой:

```sql
PRAGMA pq.Consumer="my_consumer";
```

Если читатель не указан, чтение из топика выполняется без него. Указание читателя позволяет отслеживать позицию чтения и лаг со стороны топика, например через [CLI](../reference/ydb-cli/topic-read.md).
