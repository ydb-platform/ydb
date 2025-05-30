# Universal Database

## What Is a Universal Database?

A **universal database** is a comprehensive data management system designed to handle diverse workloads and data models through a unified architecture. Similarly to [converged databases](converged-database.md), universal databases not only focus on combining transactional and analytical processing with additional capabilities, but take this concept even further by providing a complete ecosystem for all data management needs with seamless integration and native support for modern workloads.

### Key Universal Database Concepts

* **Unified data platform**: A comprehensive system that natively supports multiple data paradigms including relational, document, key-value, time-series, and vector data.
* **Workload versatility**: A single database engine capable of handling any combination of OLTP, OLAP, stream processing, vector search, and AI operations.
* **Data fabric architecture**: Consistent data access and management across distributed environments, including cloud and on-premises deployments.
* **AI-native capabilities**: Built-in support for AI workloads, including vector operations and embeddings management.
* **Cross-domain data governance**: Unified security, compliance, and governance across all data types and processing modes.

### Key {{ ydb-short-name }} Universal Database Features

* **Full [HTAP](htap.md) support**: Seamless handling of both transactional and analytical workloads.
* **[AI integration](ai-database.md)**: Native support for AI operations, including vector operations and embeddings storage.
* **[Retrieval-Augmented Generation (RAG)](rag.md)**: Built-in capabilities for implementing AI applications that combine database retrieval with generative AI.
* **Advanced streaming with [Topics](topic.md)**: Enterprise-grade publish/subscribe messaging system with exactly-once semantics and ordering guarantees.
* **Comprehensive [Vector Search](vector_search.md)**: High-performance similarity search integrated into the core query engine.
* **Extensible [Federated Queries](federated_query/index.md)**: Ability to query and combine data from diverse external sources through a unified SQL interface.
* **Cross-region [Asynchronous Replication](async-replication.md)**: Replicating data between geographically distributed clusters with minimal latency.
* **Event-driven architecture with [CDC](cdc.md)**: Capturing and processing data changes in real-time for event-driven applications.

## Scalability in Universal Databases

Universal databases implement advanced scaling strategies that work together to provide exceptional performance and reliability. The elastic scalability feature dynamically adjusts resources based on workload demands, ensuring optimal performance at all times. This is enhanced by sharding, which optimizes data distribution based on access patterns to minimize latency and maximize throughput. Native support for geo-distributed deployments allows data to be available whenever it's needed even in case of datacenter outages. Critical workloads are protected through resource isolation, which guarantees performance through fine-grained resource governance. Additionally, multi-layered storage tiering can place data on appropriate storage mediums based on access patterns, balancing cost and performance requirements.

## Universal Database Use Cases

### AI-Powered Applications

Modern AI applications require [extensive data management capabilities](ai-database.md). Universal databases excel in this area by storing and managing vector embeddings for machine learning models efficiently. They enable developers to combine semantic similarity search with traditional predicates, creating powerful hybrid query capabilities. This foundation makes it possible to support sophisticated [RAG](rag.md) applications with real-time data access, delivering more accurate and contextually relevant AI responses.

### Unified Enterprise Data Platform

A universal database serves as a comprehensive foundation for enterprise data needs. By replacing specialized systems with a single universal platform, organizations can significantly reduce complexity and maintenance overhead. This consolidation eliminates the need for data movement between disparate systems, reducing latency and potential points of failure. Additionally, it provides consistent security and governance across all data, simplifying compliance efforts and improving overall data protection.

### Event-Driven Architectures

Event-driven systems benefit greatly from the capabilities of universal databases. They provide the foundation to build reactive systems with real-time data processing, enabling immediate responses to changing conditions. The platform supports implementing event sourcing patterns with guaranteed ordering, ensuring reliable event sequence reconstruction. Additionally, universal databases connect event producers and consumers through a reliable message bus, facilitating smooth and dependable communication between system components.

## Choosing a Universal Database Solution

When evaluating universal database platforms, consider these factors:

1. **Data model requirements**: Assess your needs for relational, document, and vector data models
2. **Workload diversity**: Consider the range of processing functionality required by your applications
3. **Deployment environment**: Evaluate your needs across cloud and on-premises deployments
4. **Scalability requirements**: Project future growth in data volume, velocity, and variety
5. **Ecosystem integration**: Assess compatibility with your existing technology stack and tools

## {{ ydb-short-name }} Universal Database Features

### AI Capabilities in {{ ydb-short-name }}

{{ ydb-short-name }} offers comprehensive AI support through its vector operations for ML model integration and [similarity search](vector_search.md). The platform includes optimized storage for embeddings and high-dimensional data, ensuring efficient access and management of AI assets. To facilitate development, {{ ydb-short-name }} integrates with popular AI frameworks like [LangChain](../integrations/vectorsearch/langchain.md), enabling end-to-end machine learning workflows with minimal friction. See [{#T}](ai-database.md) for more details.

### Event Processing in {{ ydb-short-name }}

Event processing in {{ ydb-short-name }} provides [event sourcing](topic.md) with guaranteed ordering and exactly-once delivery, ensuring reliable event-based applications. The system supports real-time stream processing with low latency, enabling immediate responses to business events. This capability is enhanced through integration with [CDC](cdc.md) for capturing and processing data changes as they occur, making it easy to build reactive systems that respond to database modifications.

### Universal Query Engine in {{ ydb-short-name }}

The query engine in {{ ydb-short-name }} provides a unified SQL interface across all data models and sources, simplifying access to diverse data. It delivers sophisticated query optimization across heterogeneous data and supports complex, distributed query execution for high performance. The system extends its reach through [federated queries](federated_query/index.md) to external systems, bringing disparate data sources under a single query model. Additionally, it offers support for [JSON](../yql/reference/builtins/json.md) operations and storage, making document-oriented processing seamless and efficient.