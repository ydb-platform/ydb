# Local and external topics in streaming queries

[Streaming queries](../../concepts/streaming-query/streaming-query.md) read events from [topics](../../concepts/datamodel/topic.md) and can write results back to topics. The source and destination of messages can be a topic **in the same database** where the query is executed, or a topic **in another database** {{ ydb-short-name }}.

All use cases of [streaming queries](../../concepts/streaming-query/streaming-query.md) work the same for local{#local-topics} and external{#external-topics} topics. The same query can simultaneously read from a local topic, write to an external one, and vice versa.

## Local topics {#local-topics}

**Local topics**: topics created in the **same database** {{ ydb-short-name }} as the [streaming query](../../concepts/streaming-query/streaming-query.md).

In the query text, they are referenced **by a short name** — the same way as a table in the current database:


```yql
SELECT * FROM input_topic WITH (FORMAT = json_each_row, SCHEMA = (...));
```


```yql
INSERT INTO output_topic SELECT ...;
```


## External topics {#external-topics}

**External topics** are topics located **in another database** {{ ydb-short-name }}.

Access to them from a streaming query is only possible through a pre-created [external data source](../../concepts/datamodel/external_data_source.md) with the YDB source type. Creating the object is the CREATE EXTERNAL DATA SOURCE command; if authentication is required, secrets are used.

After creating a source, for example named `ext_source`, referencing the topic `input_topic` in the external database is written as follows:


```yql
SELECT * FROM ext_source.input_topic WITH (FORMAT = json_each_row, SCHEMA = (...));
```


The name `ext_source` in the documentation is **conditional** — in your database, the source may be named differently; it is important that it matches in `CREATE EXTERNAL DATA SOURCE` and in the prefix before the topic name.

## See also

- [Typical streaming query patterns](patterns.md) — ready-made YQL snippets
- [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md) — creating a query
- CREATE EXTERNAL DATA SOURCE — declaring a source for an external database
- [External data source](../../concepts/datamodel/external_data_source.md) — concept
- [Topic](../../concepts/datamodel/topic.md) — data model
- [Data enrichment](enrichment.md) — examples with reading from a topic and `JOIN`
- [Debug reading from a topic](../../recipes/streaming_queries/debug-read.md)
