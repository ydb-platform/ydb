# Query execution

{{ ydb-short-name }} provides a unified interface for executing queries in the [YQL](../../yql/reference/index.md) language and a unified distributed execution engine. The same syntax and mechanisms allow accessing data of different nature: from rows in database tables to unbounded streams of events in topics and data in external systems.

From the user's perspective, queries are executed against three types of entities:

- [database tables](#tables) — transactional (OLTP) and analytical (OLAP) queries to data stored in {{ ydb-short-name }}.
- [external data sources](#federated) — federated queries to data located outside {{ ydb-short-name }}.
- [topics](#streaming) — queries to unbounded data streams: streaming queries and infinite queries to topics.

The general query processing flow, as well as basic concepts — sessions, transactions, retries, query language, and result sets — are described in the [{#T}](execution_process.md) section.

## Queries to database tables {#tables}

The main scenario is executing queries against [tables](../datamodel/table.md) stored in {{ ydb-short-name }}. The unified interface can efficiently handle a wide range of workloads — from high-load [transactional OLTP queries](https://en.wikipedia.org/wiki/Online_transaction_processing) to complex [analytical OLAP queries](https://en.wikipedia.org/wiki/Online_analytical_processing).

## Queries to external data sources (federated queries) {#federated}

[Federated queries](federated_query/index.md) allow accessing data located in external systems without moving that data into {{ ydb-full-name }}. Using YQL queries, you can read data from external DBMSs and object storages (S3), and also combine it with data in {{ ydb-short-name }} tables.

For more details, see the [{#T}](federated_query/index.md) section.

## Queries to topics {#streaming}

Queries to [topics](../datamodel/topic.md) allow processing unbounded data streams. Since the data stream is infinite, such a query does not terminate after obtaining a result but runs until explicitly canceled. There are two types of such queries.

### Streaming queries

[Streaming queries](../streaming-query.md) are the primary method for streaming data processing in production. They are created as persistent schema objects (`STREAMING QUERY`), read messages from topics as they arrive, write results to output topics or tables, and automatically recover from failures using [checkpoints](../../dev/streaming-query/checkpoints.md).

For more details, see the [{#T}](../streaming-query.md) section and the [{#T}](../../yql/reference/syntax/create-streaming-query.md) description.

### Queries to topics in table mode

You can also read data from a topic using a regular `SELECT` by specifying `STREAMING = "TRUE"` in the `WITH` clause. Without the `LIMIT` limit, such a query runs indefinitely, returning results to the client as messages arrive. Unlike streaming queries, it does not create a persistent schema object and does not recover from failures, so it is primarily intended for debugging and checking data in a topic.

For more details, see the [{#T}](../../yql/reference/syntax/select/streaming.md) section.
