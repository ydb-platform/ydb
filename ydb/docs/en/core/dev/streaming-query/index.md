# Streaming queries

Practical aspects of working with [streaming queries](../../concepts/glossary.md#streaming-query):

- [Local and external topics](../../concepts/query_execution/topics.md#local-external-topics) — accessing topics of the current and another database in YQL queries
- [Typical patterns](patterns.md) — minimal examples for a quick start
- [Writing to tables](table-writing.md) — how streaming queries allow writing data to {{ ydb-short-name }} tables in real time.
- [Data enrichment](enrichment.md) — ways to enrich data in a stream using reference data.
- [Data formats for reading/writing topics](streaming-query-formats.md) — supported data formats when working with topics, examples of their use.
- [Data delivery guarantees](guarantees.md) — level of guarantees, observed anomalies in window aggregation, and recommendations.
- [Checkpoints](checkpoints.md) — a mechanism for saving the state of stream processing to ensure fault tolerance and recovery capability.
- Watermarks — a mechanism for tracking time progress in a data stream.

## See also

- [Recipes for working with streaming queries](../../recipes/streaming_queries/index.md)
- Description of streaming queries
