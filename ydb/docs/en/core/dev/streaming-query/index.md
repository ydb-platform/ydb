# Streaming queries

Practical aspects of working with [streaming queries](../../concepts/glossary.md#streaming-query):

- [Typical patterns](patterns.md) — minimal examples for a quick start
- [Writing to tables](table-writing.md) — how streaming queries allow writing data to {{ ydb-short-name }} tables in real time.
- [Data enrichment](enrichment.md) — ways to enrich data in a stream using external sources.
- [Data formats when reading/writing topics](streaming-query-formats.md) — supported data formats when working with topics, examples of their use.
- [Data delivery guarantees](guarantees.md) — level of guarantees, observed anomalies in window aggregation, and recommendations.
- [Checkpoints](checkpoints.md) — a mechanism for saving the state of stream processing to ensure fault tolerance and recovery capability.

## See also

- [Streaming query recipes](../../recipes/streaming_queries/index.md)
- [Streaming query description](../../concepts/streaming-query.md)
