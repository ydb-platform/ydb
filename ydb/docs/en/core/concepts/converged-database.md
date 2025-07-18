# Converged Database

## What Is a Converged Database?

[Hybrid Transactional/Analytical Processing (HTAP)](htap.md) allows organizations to run both transactional and analytical workloads on the same database. Similarly to [universal databases](universal-database.md), a **converged database** extends this concept by supporting multiple data models and workloads within a single system. {{ ydb-short-name }} implements this approach by providing [HTAP capabilities](htap.md) along with additional features like [topics](topic.md) for streaming, [vector search](vector_search.md), and [federated queries](federated_query/index.md).

### Key Converged Database Concepts

* **Multi-model database architecture**: A unified platform that supports different data models including rows, documents, key-value pairs, and vectors.
* **Multi-workload processing**: A single system handling OLTP, OLAP, stream processing, and vector search workloads.
* **Single source of truth**: One consistent database with immediate data availability without ETL delays.
* **Workload isolation**: Resource governance to prevent analytical workloads from impacting transaction processing.
* **Extensibility**: Ability to access external data sources without complex integration.

### Key Converged Database Features

* **OLTP+OLAP workloads** support like in [HTAP](htap.md).
* **[Topics](topic.md)**: Distributed publish/subscribe messaging with exactly-once delivery and FIFO guarantees for streaming data processing.
* **[Vector Search](vector_search.md)**: Vector search capabilities integrated into the query planner for similarity-based retrieval.
* **[Federated Query](federated_query/index.md)**: SQL queries over external data sources such as [S3-compatible object stores](federated_query/s3/external_data_source.md), [ClickHouse](federated_query/clickhouse.md), and [PostgreSQL](federated_query/postgresql.md).
* **[Change Data Capture (CDC)](cdc.md)**: Capture table changes and deliver them to topics for downstream applications to consume.
* **[Cross-Cluster Asynchronous Replication](async-replication.md)**: Replicate data between clusters for region-wide disaster recovery and improving latencies with geographic distribution.

## Scalability in Converged Databases

Converged database architectures demand scaling for both storage and compute:

* Automatic sharding keeps partitions at optimal sizes for performance
* Storage and compute separation allows the cluster to scale processing capacity independently
* Multi-version concurrency control ([MVCC](mvcc.md)) ensures wide scans don't block transactional writers

## Converged Database Use Cases

### Real-time Data Processing

* Capture operational data changes with [CDC](cdc.md)
* Process data streams using [topics](topic.md)
* Analyze data in real-time with SQL queries

### Search and Recommendations

* Store and query vector embeddings with [vector search](vector_search.md)
* Combine relational data with similarity search for enhanced results
* Execute complex queries across multiple data models

### Data Integration

* Connect to external data sources with [federated queries](federated_query/index.md)
* Maintain a single interface for accessing diverse data systems
* Eliminate complex ETL processes

## Choosing a Converged Database Solution

When evaluating converged database platforms, consider these factors:

1. **Workload requirements**: Assess your needs for transactions, analytics, streaming, and vector search
2. **Consistency guarantees**: Determine the level of consistency required for your application
3. **Operational complexity**: Consider deployment and management requirements
4. **Scalability**: Evaluate throughput, storage, and growth projections
5. **Integration capabilities**: Check compatibility with your existing systems

## {{ ydb-short-name }} Converged Features

### Topics in {{ ydb-short-name }}

* [Exactly-once delivery](topic.md#autopartitioning_guarantee) semantics for reliable message processing
* [Auto-partitioning](topic.md#autopartitioning) for throughput scaling while preserving order within source ID
* Integration with transactions to read and write to topics alongside tables

### Vector Search in {{ ydb-short-name }}

* Vector search capabilities via [dedicated functions](vector_search.md#vector-search-exact) for similarity-based retrieval
* Support for both [exact](vector_search.md#vector-search-exact) and [approximate](vector_search.md#vector-search-approximate) vector search methods
* Integration with the SQL query planner for combined relational and vector operations

### Additional {{ ydb-short-name }} Capabilities

* **JSON Support**: Store and [query JSON data](../yql/reference/builtins/json.md) along with relational data
* **Time-Series**: Table [TTL](ttl.md) for efficient management of data archival in time-series use cases
* **[Federated Queries](federated_query/index.md)**: Access data in external systems through a unified SQL interface