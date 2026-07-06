# Local and external topics in streaming queries

[Streaming queries](../../concepts/streaming-query.md) read events from [topics](../../concepts/datamodel/topic.md) and can write results back to topics. Messages can come from or go to a topic **in the same database** where the query runs, or **in another** {{ ydb-short-name }} database.

All [streaming query](../../concepts/streaming-query.md) scenarios work the same for [local](#local-topics) and [external](#external-topics) topics. A single query can read a local topic, write to an external one, and vice versa.

## Local topics {#local-topics}

**Local topics** are topics created in the **same** {{ ydb-short-name }} database as the [streaming query](../../concepts/streaming-query.md).

In query text you refer to them **by short name**, the same way as to a table in the current database:

```yql
SELECT * FROM input_topic WITH (FORMAT = json_each_row, SCHEMA = (...));
```

```yql
INSERT INTO output_topic SELECT ...;
```

## External topics {#external-topics}

**External topics** are topics in **another** {{ ydb-short-name }} database.

Access from a streaming query goes only through a pre-created [external data source](../../concepts/datamodel/external_data_source.md) with source type YDB. Create it with [CREATE EXTERNAL DATA SOURCE](../../yql/reference/syntax/create-external-data-source.md); use [secrets](../../yql/reference/syntax/create-secret.md) when authentication is required.

After you create a source, for example named `ext_source`, a topic `input_topic` in the external database is referenced as:

```yql
SELECT * FROM ext_source.input_topic WITH (FORMAT = json_each_row, SCHEMA = (...));
```

The name `ext_source` in the documentation is **conventional** — your source may have a different name; it must match in `CREATE EXTERNAL DATA SOURCE` and in the prefix before the topic name.

## See also

- [Common streaming query patterns](patterns.md) — ready-to-use YQL fragments
- [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md) — creating a query
- [CREATE EXTERNAL DATA SOURCE](../../yql/reference/syntax/create-external-data-source.md) — declaring a source for an external database
- [External data source](../../concepts/datamodel/external_data_source.md) — concept
- [Topic](../../concepts/datamodel/topic.md) — data model
- [Data enrichment](enrichment.md) — examples with topic read and `JOIN`
- [Debug read from a topic](../../recipes/streaming_queries/debug-read.md)
