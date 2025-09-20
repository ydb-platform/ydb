# {{ ydb-short-name }} Architecture Overview

## Introduction

{{ ydb-short-name }} is a horizontally scalable, distributed, and fault-tolerant database system designed as a versatile platform for high performance — for example, a typical cluster node can process tens of thousands of queries per second. The system supports geographically distributed (cross-datacenter) configurations, ranging from small clusters with a few nodes to large-scale deployments of thousands of servers capable of efficiently handling hundreds of petabytes of data.

### Key Features and Capabilities of {{ ydb-short-name }}

- **Horizontal scaling and automatic sharding**: data and workload are dynamically distributed across available hardware resources as data volume or query intensity grows.
- **Fault tolerance**: automatic recovery from failures of nodes, racks, or availability zones.
- **Data high availability and durability**: ensured through automatic synchronous data replication within the cluster.
- **Strong consistency and ACID transactions**: the system provides [distributed transactions](transactions.md) with *serializable* isolation. Consistency and isolation levels can be relaxed when higher performance is required.
- [**YQL**](../yql/reference/index.md): a SQL dialect optimized for large-scale data and complex processing scenarios.
- **Relational data model**: supports both [row-oriented](datamodel/table.md#row-oriented-tables) and [column-oriented](datamodel/table.md#column-oriented-tables) tables, enabling efficient handling of both transactional (OLTP) and analytical (OLAP) workloads within a single system.
- **Hierarchical namespace**: tables, topics, and other [objects](datamodel/index.md) are organized in a hierarchical namespace, similar to a filesystem.
- [**Asynchronous replication**](async-replication.md): near real-time data synchronization between {{ ydb-short-name }} databases — both within a single cluster and across different clusters.
- **Streaming data processing and distribution**:
  - **Topics**: storage and streaming delivery of unstructured messages to multiple subscribers. Supports the [Kafka protocol](../reference/kafka-api/index.md).
  - [**Change Data Capture (CDC)**](cdc.md): built-in stream of table data changes published as a topic.
  - **Transfers**: automated data delivery from topics to tables.
- [**Federated queries**](federated_query/index.md): execute queries against external data sources (e.g., S3) as part of YQL queries, without prior data import to {{ ydb-short-name }} storage.
- [**Vector indexes**](vector_search.md): support for storing and searching vector embeddings — ideal for semantic search, similarity matching, and ML use cases.
- [**Observability**](../reference/observability/index.md): built-in metrics, logs, and dashboards.
- **Security and audit**: data encryption (at-rest and in-transit), operation auditing, and support for authentication and authorization — see [Security](../security/index.md).
- **Tools, integrations, and APIs**: [{{ ydb-short-name }} CLI](../reference/ydb-cli/index.md) for running queries, administration, and debugging. [SDKs](../reference/ydb-sdk/index.md) for C++, C#, Go, Java, Node.js, PHP, Python, and Rust. Integrations with various third-party systems. Learn more in [{#T}](../integrations/index.md) and [{#T}](../reference/languages-and-apis/index.md).
- **Open architecture**: [source code](https://github.com/ydb-platform/ydb) is available under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). The system uses the open [gRPC](https://grpc.io/) protocol, enabling client implementations in any programming language.

### Key Use Cases

{{ ydb-short-name }} is a versatile platform suitable for a wide range of scenarios requiring scalability, reliability, and flexibility. Typical use cases include:

- In distributed systems requiring **strong consistency or support for multi-row and multi-table transactions**. {{ ydb-short-name }} combines NoSQL-like scalability with the consistency and integrity guarantees of relational databases.
- Systems that store and process **very large datasets** and require nearly unlimited horizontal scaling (production clusters with thousands of nodes, handling millions of RPS and petabytes of data).
- High-load systems relying on **manual sharding** of relational databases. {{ ydb-short-name }} simplifies architecture by automatically handling the sharding logic, re-sharding, query routing, and cross-shard transactions out of the box.
- New product development with **uncertain load patterns** or expected scale beyond the limits of traditional relational database management systems (RDBMS).
- Projects requiring a **flexible platform** capable of handling diverse workloads and use cases — including transactional, streaming, and analytical.

## How It Works?

Fully explaining how YDB works in detail takes quite a while. Below you can review several key highlights and then continue exploring the documentation to learn more.

### {{ ydb-short-name }} Architecture {#ydb-architecture}

![YDB architecture](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/grps.png)

{{ ydb-short-name }} clusters typically run on commodity hardware with a shared-nothing architecture. From a bird's eye view, {{ ydb-short-name }} exhibits a layered architecture. The compute and storage layers are disaggregated; they can either run on separate sets of nodes or be co-located.

One of the key building blocks of {{ ydb-short-name }}'s compute layer is called a *tablet*. Tablets are stateful logical components implementing various aspects of {{ ydb-short-name }}.

The next level of detail of the overall {{ ydb-short-name }} architecture is explained in the [{#T}](../contributor/general-schema.md) article.

### Hierarchy {#ydb-hierarchy}

![Hierarchy](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/organization.png)

From the user's perspective, everything inside {{ ydb-short-name }} is organized in a hierarchical structure using directories. It can have arbitrary depth depending on how you choose to organize your data and projects. Even though {{ ydb-short-name }} does not have a fixed hierarchy depth like in other SQL implementations, it will still feel familiar as this is exactly how any virtual filesystem looks.

### Table {#table}

![Table](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/table.png)

{{ ydb-short-name }} provides users with a well-known abstraction — tables. In {{ ydb-short-name }}, there are two main types of tables:

* [Row-oriented tables](datamodel/table.md#row-tables) are designed for OLTP workloads.
* [Column-oriented tables](datamodel/table.md#column-tables) are designed for OLAP workloads.

Logically, from the user's perspective, both types of tables look the same. The main difference between row-oriented and column-oriented tables lies in how the data is physically stored. In row-oriented tables, the values of all columns in each row are stored together. In contrast, in column-oriented tables, each column is stored separately, meaning that cells from different rows are stored next to each other within the same column.

Regardless of the type, each table must have a primary key. Column-oriented tables can only have `NOT NULL` columns in primary keys. Table data is physically sorted by the primary key.

Partitioning works differently in row-oriented and column-oriented tables:

* Row-oriented tables are automatically partitioned by primary key ranges, depending on the data volume.
* Column-oriented tables are partitioned by the hash of the partitioning columns.

Each partition of a table is processed by a specific [tablet](glossary.md#tablets), called a [data shard](glossary.md#datashard) for row-oriented tables and a [column shard](glossary.md#columnshard) for column-oriented tables.

#### Split by Load {#split-by-load}

![Split by load](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/nagruz%201.5.png)

Data shards will automatically split into more as the load increases. They automatically merge back to the appropriate number when the peak load subsides.

#### Split by Size {#split-by-size}

![Split by size](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/size%201.5%20(1).png)

Data shards will also automatically split when the data size increases. They automatically merge back if enough data is deleted.

### Automatic Balancing {#automatic-balancing}

![Automatic balancing](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/pills%201.5.png)

{{ ydb-short-name }} evenly distributes tablets among available nodes. It moves heavily loaded tablets from overloaded nodes. CPU, memory, and network metrics are tracked to facilitate this.

### Distributed Storage Internals {#ds-internals}

![Distributed Storage internals](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/distributed.png)

{{ ydb-short-name }} doesn't rely on any third-party filesystem. It stores data by directly working with disk drives as block devices. All major disk kinds are supported: NVMe, SSD, or HDD. The PDisk component is responsible for working with a specific block device. The abstraction layer above PDisk is called VDisk. There is a special component called DSProxy between a tablet and VDisk. DSProxy analyzes disk availability and characteristics and chooses which disks will handle a request and which won't.

### Distributed Storage Proxy (DSProxy) {#ds-proxy}

![DSProxy](https://storage.yandexcloud.net/ydb-www-prod-site-assets/howitworks/proxy%202.png)

A common fault-tolerant setup of {{ ydb-short-name }} spans three datacenters or availability zones (AZ). When {{ ydb-short-name }} writes data to three AZs, it doesn't send requests to obviously bad disks and continues to operate without interruption even if one AZ and a disk in another AZ are lost.

## What's Next?

If you are interested in more specifics about various aspects of YDB, check out neighboring articles in this documentation section. If you are ready to jump into more practical content, you can continue to the [quick start](../quickstart.md) or [YQL](../dev/yql-tutorial/index.md) tutorials.