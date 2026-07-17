# Streaming queries

Practical aspects of working with [streaming queries](../../concepts/glossary.md#streaming-query):

- [Using local and external topics](local-and-external-topics.md)
- [Typical patterns](patterns.md) — minimal examples for a quick start
- [Writing to tables](table-writing.md) — how streaming queries allow writing data to {{ ydb-short-name }} tables in real time.
- [Data enrichment](enrichment.md) — ways to enrich data in a stream using reference books.
- [Data formats when reading/writing topics](streaming-query-formats.md) — supported data formats when working with topics, examples of their use.
- [Data delivery guarantees](guarantees.md) — level of guarantees, observed anomalies during window aggregation, and recommendations.
- [Checkpoints](checkpoints.md) — a mechanism for saving the stream processing state to ensure fault tolerance and recovery capability.
- [Watermarks](watermarks.md) — a mechanism for tracking time progress in a data stream.

## See also

- [Recipes for working with streaming queries](../../recipes/streaming_queries/index.md)
- [Description of streaming queries](../../concepts/streaming-query/streaming-query.md)
