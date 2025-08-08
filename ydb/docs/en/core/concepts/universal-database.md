# Universal Database

## What Is a Universal Database?

A **universal database** is a database system that supports multiple data models and workload types within a single platform. These systems extend beyond traditional [OLTP](https://en.wikipedia.org/wiki/Online_transaction_processing) and [OLAP](https://en.wikipedia.org/wiki/Online_analytical_processing) systems by integrating additional capabilities such as vector search, stream processing, and document storage.

### Key Concepts

* **Multi-model support**: Support for relational, document, key-value, time-series, and vector data within a single system
* **Workload consolidation**: Processing of [OLTP](https://en.wikipedia.org/wiki/Online_transaction_processing), [OLAP](https://en.wikipedia.org/wiki/Online_analytical_processing), stream processing, and vector search operations using shared infrastructure
* **Unified query interface**: Single query language and Application Programming Interface (API) for accessing different data models and processing modes
* **Integrated storage**: Shared storage layer that handles different data types without requiring separate systems
* **Cross-workload governance**: Consistent security, compliance, and administration across all supported workloads

### Key Features of {{ ydb-short-name }} as a Universal Database

* **Full support for both [OLTP](https://en.wikipedia.org/wiki/Online_transaction_processing) and [OLAP](https://en.wikipedia.org/wiki/Online_analytical_processing)**: Seamless handling of both transactional and analytical workloads.
* **Multi-Model Support**: [Vector operations](vector_search.md) for similarity search and ML model integration, [JSON operations](../yql/reference/builtins/json.md) and document storage, time-series data handling with temporal queries, and integration with [AI frameworks](../integrations/vectorsearch/langchain.md) such as LangChain.
* **Event Processing Capabilities**: [Topics](topic.md) for event streaming with ordering guarantees and exactly-once delivery, [Change Data Capture (CDC)](cdc.md) for real-time data change tracking, and event sourcing patterns with persistent event storage.
* **Query Processing**: Unified SQL interface across all supported data models, [Federated queries](federated_query/index.md) for accessing external data sources, and distributed query execution with cross-model optimization.
* **[AI integration](ai-database.md)**: Native support for AI operations, including vector operations and embeddings storage.
* **[Retrieval-Augmented Generation (RAG)](rag.md)**: Built-in capabilities for implementing AI applications that combine database retrieval with generative AI.
* **Advanced streaming with [Topics](topic.md)**: Enterprise-grade publish/subscribe messaging system with exactly-once semantics and ordering guarantees.
* **Comprehensive [Vector Search](vector_search.md)**: High-performance similarity search integrated into the core query engine.
* **Extensible [Federated Queries](federated_query/index.md)**: Ability to query and combine data from diverse external sources through a unified SQL interface.
* **Cross-region [Asynchronous Replication](async-replication.md)**: Replicating data between geographically distributed clusters with minimal lag.
* **Event-driven architecture with [CDC](cdc.md)**: Capturing and processing data changes in real-time for event-driven applications.

## Scaling Considerations

Universal databases face unique scaling challenges due to their multi-workload nature:

* **Resource allocation**: Different workloads (OLTP, OLAP, vector search) have varying resource requirements and access patterns
* **Data distribution**: Sharding strategies must account for multiple data models and query types
* **Performance isolation**: Preventing resource contention between concurrent workload types
* **Storage tiering**: Managing different storage requirements for transactional, analytical, and vector data
* **Geographic distribution**: Coordinating distributed operations across multiple data models and workload types

## Common Use Cases

### Applications Requiring Multiple Data Models

Systems that need to handle structured transactions alongside document storage, vector search, or time-series data benefit from consolidation in a universal database. Examples include:

* E-commerce platforms combining transactional data, product catalogs (documents), and recommendation engines (vectors)
* IoT systems processing time-series sensor data alongside relational metadata and event streams
* Content management systems storing structured metadata, document content, and similarity-based search indexes

### AI and Machine Learning Workloads

Universal databases support [AI applications](ai-database.md) that require:

* Vector storage and similarity search for embeddings
* Hybrid queries combining vector search with traditional predicates
* [RAG](rag.md) implementations requiring fast retrieval alongside transactional data
* Real-time feature stores combining historical and streaming data

### Event-Driven Systems

Applications using event-driven architectures can leverage universal databases for:

* Event sourcing with ordered event storage and replay capabilities
* [Change Data Capture (CDC)](cdc.md) for real-time data synchronization
* Stream processing integrated with persistent state management
* Message queuing with transactional guarantees

## Implementation Considerations

When implementing or selecting a universal database system, key factors include:

1. **Data model requirements**: Determine which combinations of relational, document, vector, and time-series models are needed
2. **Workload characteristics**: Analyze the mix of transactional, analytical, and specialized processing requirements
3. **Performance requirements**: Consider latency, throughput, and consistency needs across different workload types
4. **Resource management**: Evaluate requirements for workload isolation and resource allocation
5. **Operational complexity**: Assess the trade-offs between consolidation benefits and increased system complexity