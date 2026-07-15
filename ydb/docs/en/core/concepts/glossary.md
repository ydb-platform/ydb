# Glossary {{ ydb-short-name }}

This article provides an overview of the terms and definitions used in {{ ydb-short-name }} and its documentation. It [starts with key terms](#key-terminology) that are useful to get familiar with early when working with {{ ydb-short-name }}, and the rest of the article contains [more advanced terms](#advanced-terminology) that may be helpful later.

## Key terminology {#key-terminology}

This section describes terms that are useful to anyone working with {{ ydb-short-name }}, regardless of their role or use case.

### Cluster {#cluster}

**cluster** {{ ydb-short-name }} is a set of interrelated [nodes](#node) {{ ydb-short-name }} that exchange data to execute user queries and reliably store data. These nodes form one of the supported [cluster topologies](#topology), directly influencing its reliability and performance characteristics.

Clusters {{ ydb-short-name }} are multi‑tenant (multi‑user) and can contain multiple isolated [databases](#database).

### Database {#database}

As in most database management systems, a **database** in {{ ydb-short-name }} is a logical container for other entities such as [tables](#table). However, in {{ ydb-short-name }} the namespace within databases is hierarchical, like in [virtual file systems](https://en.wikipedia.org/wiki/Virtual_file_system), and thus [directories](#folder) allow more structured organization of entities.

Another important characteristic of databases {{ ydb-short-name }} is that they are usually allocated separate compute resources. Consequently, creating a database requires additional actions by [DevOps engineers](../devops/index.md).

### Node {#node}

{{ ydb-short-name }} **node** is a server process that runs the executable named `ydbd`. Several nodes {{ ydb-short-name }} can run on a single physical server or virtual machine, which is common practice. Thus, in the context of {{ ydb-short-name }}, nodes **are not** synonyms of hosts.

Since {{ ydb-short-name }} uses a storage and compute separation approach, `ydbd` has several operating modes that define the node type. The available node types are described below.

#### Database node {#database-node}

**Database nodes** (also known as **tenant nodes** or **compute nodes**) process user queries addressed to a specific logical [database](#database). Their state resides only in memory and can be restored from [distributed storage](#distributed-storage). The set of database nodes of a given [cluster {{ ydb-short-name }}](topology.md) can be considered the compute layer of that cluster. Thus, adding database nodes and allocating them additional resources (CPU and RAM) are the primary ways to increase the compute resources of a database.

The primary role of database nodes is to run various [tablets](#tablet) and [actors](#actor), as well as to accept incoming network requests.

#### Storage node {#storage-node}

**Storage nodes** are stateful nodes responsible for long‑term storage of data fragments. The set of storage nodes of a given [cluster {{ ydb-short-name }}](#cluster) is called [distributed storage](#distributed-storage) and can be viewed as the storage layer of that cluster. Thus, adding additional storage nodes and their disks are the primary ways to increase the storage capacity and I/O throughput of the cluster.

#### Hybrid node {#hybrid-mode}

**Hybrid node** is a process that simultaneously performs both the roles of a [database node](#database-node) and a [storage node](#storage-node). Hybrid nodes are often used for development purposes. For example, you can run a container with a full‑featured {{ ydb-short-name }} containing only a single `ydbd` process in hybrid mode. In production environments they are rarely used.

#### Static node {#static-node}

**Static nodes** are manually configured during the initial initialization or reconfiguration of a cluster. Typically, they serve as [storage nodes](#storage-node), but it is technically possible to configure them as [database nodes](#database-node) as well.

#### Dynamic node {#dynamic}

**dynamic nodes** are added and removed from the cluster on the fly. They can serve only as [database nodes](#database-node).

### Distributed storage {#distributed-storage}

**Distributed storage**, **Blob storage**, or **BlobStorage** is a distributed fault‑tolerant data storage layer in {{ ydb-short-name }}. It has a specialized API designed for storing immutable data fragments of [tablet](#tablet).

A number of terms related to [the implementation of distributed storage](#distributed-storage-implementation) are discussed below.

### Storage group {#storage-group}

**storage group**, **distributed storage group**, or **Blob storage group** is a location for reliable data storage, similar to [RAID](https://en.wikipedia.org/wiki/RAID), but using disks of multiple servers. Depending on the selected [cluster topology](#topology), storage groups employ different algorithms to ensure high availability, similar to [standard RAID levels](https://en.wikipedia.org/wiki/Standard_RAID_levels).

[Distributed storage](#distributed-storage) typically manages a large number of relatively small storage groups. Each group can be assigned to a specific [database](#database) to increase the disk space capacity and I/O throughput available to that database.

[Static](#static-group) and [dynamic](#dynamic-group) storage groups are physical, meaning their data is placed directly on [VDisk](#vdisk).

#### Static group {#static-group}

**static group** is a special [storage group](#storage-group) created during the initial deployment of the cluster. Its primary role is to store data of system [tablets](#tablet), which can be viewed as metadata for the entire cluster level.

A static group may require special attention during large‑scale cluster maintenance, such as decommissioning an [availability zone](#regions-az).

#### Dynamic group {#dynamic-group}

Regular storage groups that are not [static](#static-group) are called **dynamic groups**, or **dynamic group**. They are called dynamic because they can be created and removed on the fly during the operation of the [cluster](#cluster).

#### Virtual storage group {#virtual-storage-groups}

**virtual storage group** is an entity that is not actually a [storage group](#storage-group), but appears as one from the outside (provides a similar external interface). It can store its data in other storage groups or in S3.

### Storage pool {#storage-pool}

**storage pool** is a set of data storage devices with similar characteristics. Each storage pool is assigned a unique name within the cluster {{ ydb-short-name }}. Technically, each storage pool consists of many physical disks ( [PDisk](#pdisk)). Each [storage group](#storage-group) is created in a specific storage pool, which determines the performance characteristics of the storage group by selecting appropriate storage devices. Typically, separate storage pools are created for devices of different types (e.g., NVMe, SSD, and HDD) or for specific models of those devices that have varying capacity and access speed.

### Actor {#actor}

[Actor model](https://ru.wikipedia.org/wiki/%D0%9C%D0%BE%D0%B4%D0%B5%D0%BB%D1%8C_%D0%B0%D0%BA%D1%82%D0%BE%D1%80%D0%BE%D0%B2) is one of the primary approaches to execution parallelism used in {{ ydb-short-name }}. In this model, actors (or **actor**) are lightweight processes in user space that can have and modify their private state, but can affect each other only indirectly via message passing. {{ ydb-short-name }} has its own implementation of this model, which is described [below](#actor-implementation).

In {{ ydb-short-name }}, actors with reliably persisted state are called [tablets](#tablet).

### Tablet {#tablet}

**tablet** is one of the core building blocks and abstractions of {{ ydb-short-name }}. It represents an entity responsible for a relatively small segment of user or system data. Typically, a tablet manages data volumes up to several gigabytes, though some tablet types can handle larger volumes.

For example, a [row user table](#row-oriented-table) is managed by one or more tablets of type [DataShard](#data-shard), and each tablet is responsible for a continuous range of [primary keys](#primary-key) and the corresponding data.

End users sending requests to the {{ ydb-short-name }} cluster for execution do not need to know the details of tablets, their types, or how they work, but it can be useful, for example, for performance optimization.

Technically, tablets are [actors](#actor) with state reliably stored in [distributed storage](#distributed-storage). This state allows a tablet to continue operating on another [database node](#database-node) if the previous one fails or is overloaded.

[Implementation details of tablets](#tablet-implementation) and related terms, as well as [primary tablet types](#tablet-types), are discussed below.

### Transactions {#transactions}

{{ ydb-short-name }} implements **transactions** (**transactions**) on two primary levels:

* [Local database](#local-database) and the rest of the [tablet infrastructure](#tablet-implementation) allow [tablets](#tablet) to manipulate their state using **local transactions** with [serializable isolation level](https://ru.wikipedia.org/wiki/%D0%A3%D1%80%D0%BE%D0%B2%D0%B5%D0%BD%D1%8C_%D0%B8%D0%B7%D0%BE%D0%BB%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%BD%D0%BE%D1%81%D1%82%D0%B8_%D1%82%D1%80%D0%B0%D0%BD%D0%B7%D0%B0%D0%BA%D1%86%D0%B8%D0%B9#Serializable_(%D1%83%D0%BF%D0%BE%D1%80%D1%8F%D0%B4%D0%BE%D1%87%D0%B8%D0%B2%D0%B0%D0%B5%D0%BC%D0%BE%D1%81%D1%82%D1%8C)). Technically they are not local to a single node, because this state is stored remotely in [distributed storage](#distributed-storage).
* In the context of {{ ydb-short-name }}, the term **distributed transactions** usually refers to transactions that span multiple tablets. For example, transactions between tables or even rows of a single table are often distributed.
* **single-shard** transactions cover a single tablet and execute faster. For example, transactions between rows of a single table partition are often single-shard.

These mechanisms enable {{ ydb-short-name }} to provide [strict consistency](https://en.wikipedia.org/wiki/Consistency_model#Strict_consistency).

{% if oss %}

The implementation of distributed transactions is covered in a separate article [{#T}](../contributor/datashard-distributed-txs.md), and below is a list of several [related terms](#deterministic-transactions).

{% endif %}

### Sessions

Logical connections to the database that store the context needed to execute queries and manage transactions. Sessions are described in more detail in the section [{#T}](query_execution/execution_process.md#sessions).

### Client timeout {#client-timeout}

**client-side timeout** — a time limit that the application or {{ ydb-short-name }} SDK waits for an operation with the database to complete (for example, query execution or receiving a gRPC response). When this time expires, the client typically aborts the wait: closes the connection or data stream, receives a transport or SDK error — before the server has returned an explicit response (see [codes](../reference/ydb-sdk/ydb-status-codes.md) of the server response {{ ydb-short-name }}).

If the client timeout is shorter than the query execution time on the {{ ydb-short-name }} side, then due to the way requests are processed in the cluster, a client‑aborted request may continue to run on the server for some time. If this situation occurs at scale, the server becomes overloaded with requests whose responses the client is not waiting for. Therefore, frequent retries of the same request immediately after a timeout can exacerbate the overload. See the articles [{#T}](../troubleshooting/performance/queries/retry-cascade.md) and [{#T}](../troubleshooting/performance/queries/overloaded-errors.md) for more details; retry policies in the SDK are described in the section [{#T}](../reference/ydb-sdk/error_handling.md).

### Transaction retry {#transaction-retry}

**transaction retry** — a client practice of re‑executing a [transaction](#transactions) from the beginning when an error that permits a retry occurs (for example, a temporary network failure or an optimistic lock conflict). In {{ ydb-short-name }}, retries should be performed at the transaction level, not at the level of individual queries within it. Built‑in retry policies in the SDK {{ ydb-short-name }} and integrations (for example, [spring-ydb-retry](../integrations/spring/spring-retry.md)) implement this approach. See [{#T}](../reference/ydb-sdk/error_handling.md) for more details.

### Exponential backoff {#exponential-backoff}

**exponential backoff**, **backoff** — a strategy of pausing between [transaction retry](#transaction-retry) attempts: the wait interval grows exponentially with each attempt, usually up to a maximum. The SDK {{ ydb-short-name }} often uses two backoff levels — fast and slow — depending on the error type. See [{#T}](../reference/ydb-sdk/error_handling.md#handling-retryable-errors) for more details.

### Jitter {#jitter}

**jitter** — a small random variation added to the delays of [exponential backoff](#exponential-backoff). It helps avoid simultaneous retries by many clients after a common failure (“retry storm”) and distributes load more evenly.

### Idempotency {#idempotency}

**idempotency** — a property of an operation: repeating it yields the same effect as executing it once (for example, `UPSERT` with a deterministic primary key or read operations). [Transaction retry](#transaction-retry) is safe only for idempotent operations or in retry errors where the server guarantees that the transaction was not committed. SDKs and client libraries {{ ydb-short-name }} can extend the set of retryable status codes if the calling code marks the operation as idempotent.

### Transaction interceptor {#transaction-interceptor}

**transaction interceptor** — a Spring Framework component that wraps methods annotated with `@Transactional` and manages transaction boundaries. Modules such as [spring-ydb-retry](../integrations/spring/spring-retry.md) replace the standard Spring interceptor, adding [transaction retry](#transaction-retry) logic around transactional methods.

### Implicit transactions {#implicit-transactions}

**implicit transaction** — a query execution mode where the [transaction mode](transactions.md#modes) is not specified. In this case {{ ydb-short-name }} determines automatically whether to wrap them in a transaction. This mode is described in more detail in [{#T}](transactions.md#implicit).

### Multi-version concurrency control {#mvcc}

[**multi-version concurrency control**](https://ru.wikipedia.org/wiki/MVCC), **multi-version concurrency control** or **MVCC** — a method used by {{ ydb-short-name }} for concurrent access by multiple parallel transactions to the database without mutual interference. It is described in more detail in a separate article [{#T}](query_execution/mvcc.md).

### Streaming queries {#streaming-query}

A query type intended for [stream processing](https://en.wikipedia.org/wiki/Stream_processing) of an unbounded data stream. Unlike regular queries, streaming queries have no limits on execution duration, automatically restart on errors, and periodically save their state as [checkpoints](#streaming-queries-checkpoints) to ensure fault tolerance. Event‑time progress tracking uses [watermarks](#streaming-queries-watermarks).

Streaming queries are described in more detail in a separate article [{#T}](./streaming-query/streaming-query.md).

### Streaming query checkpoints {#streaming-queries-checkpoints}

Periodically saved state of a [streaming query](#streaming-query) required for automatic recovery of its operation after failures in a distributed system. More about checkpoints in the article [{#T}](../dev/streaming-query/checkpoints.md).

### Streaming query watermarks {#streaming-queries-watermarks}

A monotonically increasing lower bound on event times in a [streaming query](#streaming-query) that may still arrive in the stream. When the watermark reaches value X, the system declares that all events with timestamps less than X have, with high probability, already been received. More about watermarks in the article [{#T}](./streaming-query/watermarks.md).

### Topology {#topology}

{{ ydb-short-name }} supports multiple **topologies** of a [cluster](#cluster) (or **topology**), described in more detail in a separate article [{#T}](topology.md). Below are explanations of several related terms.

#### Availability zones and regions {#regions-az}

**availability zone** — a data center or an isolated segment of it with minimal physical distance between nodes and minimal risk of simultaneous failure with other availability zones. Thus, availability zones should not share common infrastructure such as power, cooling, or external network connections.

**region** — a large geographic area containing multiple availability zones. The distance between availability zones within a region should be about 500 km or less. {{ ydb-short-name }} writes data to each availability zone in the region synchronously, providing reasonable latency and uninterrupted operation in case one availability zone fails.

#### Rack {#rack}

**rack** or **server rack** — is equipment used to organize placement of multiple servers. Servers in the same rack are more likely to become unavailable simultaneously due to rack-level issues such as power, cooling, etc. {{ ydb-short-name }} can take into account which server is in which rack when placing each data fragment in environments based on physical servers.

#### Pile {#pile}

**Pile** is a set of nodes that can fail or be disconnected simultaneously while the other parts of the cluster (pile) remain operational. A Pile can maintain operation when other cluster nodes are disconnected. Piles are used in the [bridge mode](#bridge) to split the cluster into multiple parts that perform synchronous replication. A Pile can consist of nodes from one or several regions.

#### Bridge mode {#bridge}

**bridge mode** is a special cluster topology in which data is stored with synchronous replication between multiple [piles](#pile). The mode’s characteristics are described in [{#T}](topology.md#bridge) and also in [{#T}](bridge.md).

### Table {#table}

**table** — a structured fragment of information organized into rows and columns. Each row represents a single record or element, and each column is a specific attribute or field with a defined data type.

There are two main approaches to representing tabular data in memory or on disk: [row-oriented (row by row)](#row-oriented-table) and [column-oriented (column by column)](#column-oriented-table). The chosen approach strongly affects performance characteristics of operations on this data: the first is better suited for transactional workloads (OLTP), while the second is for analytical (OLAP). {{ ydb-short-name }} supports both approaches.

#### Row-oriented table {#row-oriented-table}

**row-oriented tables** store data for all or most columns of each row physically together. They are described in more detail in [{#T}](datamodel/table.md#row-oriented-tables).

#### Column-oriented table {#column-oriented-table}

**column-oriented table**, **columnar table** store data for each column separately. They are optimized for building aggregates over a small number of columns but are less suitable for accessing specific rows, as rows must be reconstructed from their cells on the fly. They are described in more detail in [{#T}](datamodel/table.md#column-oriented-tables).

#### Primary key {#primary-key}

**primary key** is an ordered list of columns whose values uniquely identify a row. It is used to create the table’s [primary index](#primary-index). It is specified by the user {{ ydb-short-name }} when [creating a table](../yql/reference/syntax/create_table/index.md) and significantly impacts the performance of operations on that table.

A guide to choosing primary keys is presented in [{#T}](../dev/primary-key/index.md).

#### Primary index {#primary-index}

**primary index**, **primary key index** — is the main data structure used to locate rows in a table. It is built based on the selected [primary key](#primary-key) and determines the physical order of rows in the table; thus, each table can have only one primary index. A primary index is unique.

#### Secondary index {#secondary-index}

**secondary index** — is an additional data structure used to locate rows in a table, typically when this cannot be done efficiently with the [primary index](#primary-index). Unlike the primary index, secondary indexes are managed independently of the table’s main data. Thus, a table can have multiple secondary indexes for different scenarios. {{ ydb-short-name }}’s capabilities regarding secondary indexes are described in a separate article [{#T}](query_execution/secondary_indexes.md). A secondary index can be either unique or non-unique.

Special types of secondary indexes are highlighted separately — [vector index](#vector-index), [full-text index](#fulltext-index) and [JSON index](#json-index).

#### Vector index {#vector-index}

**vector index** is an additional data structure used to accelerate solving the task of [vector search](query_execution/vector_search.md) when there is a large amount of data and [exact vector search without an index](../yql/reference/udf/list/knn.md) does not work satisfactorily.
The capabilities {{ ydb-short-name }} for approximate nearest neighbor (ANN) search using vector indexes are described in a separate article [{#T}](../dev/vector-indexes.md).

**vector index** — is a specialized type of [secondary index](#secondary-index) designed for similarity-based search, unlike traditional secondary indexes that are optimized for equality or range searches.

#### Full-text index {#fulltext-index}

**full-text index** — it is an additional data structure used to accelerate text search on a table column (by words and phrases, and when using n-grams — also by substrings).

Full-text search capabilities and index parameters are described in the articles [{#T}](../dev/fulltext-indexes.md) and [{#T}](query_execution/fulltext_search.md).

#### JSON index {#json-index}

**JSON index** — it is an additional data structure used to accelerate predicates with functions [JSON_EXISTS](../yql/reference/builtins/json.md#json_exists) and [JSON_VALUE](../yql/reference/builtins/json.md#json_value) on a column of type `Json` or `JsonDocument`. Unlike traditional secondary indexes, which are optimized for equality or range searches on individual table columns, a JSON index works with arbitrary [JsonPath](../yql/reference/builtins/json.md#jsonpath) paths inside a JSON document.

The JSON index, like the [full-text index](#fulltext-index), is built on top of the [inverted index](https://ru.wikipedia.org/wiki/%D0%98%D0%BD%D0%B2%D0%B5%D1%80%D1%82%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%BD%D1%8B%D0%B9_%D0%B8%D0%BD%D0%B4%D0%B5%D0%BA%D1%81) but uses its own tokenizer for JSON documents. JSON search capabilities are described in the articles [{#T}](../dev/json-indexes.md) and [{#T}](query_execution/json_search.md).

#### Local index {#local-index}

Local index — an auxiliary structure that is stored together with the table data (as opposed to the [global secondary index](#secondary-index), which materializes a separate index table). Local index is used when reading the main table on the storage side. More details: [local indexes](query_execution/local_indexes.md).

#### Bloom filter {#bloom-filter}

A Bloom filter — a [probabilistic data structure](https://ru.wikipedia.org/wiki/%D0%A4%D0%B8%D0%BB%D1%8C%D1%82%D1%80_%D0%91%D0%BB%D1%83%D0%BC%D0%B0) that allows you to quickly check whether an element belongs to a set. False positives are possible, but false negatives are not.

#### Local bloom index {#local-bloom-skip-index}

Local Bloom index — a special case of [local index](#local-index): a probabilistic filter on column values based on a [Bloom filter](https://ru.wikipedia.org/wiki/%D0%A4%D0%B8%D0%BB%D1%8C%D1%82%D1%80_%D0%91%D0%BB%D1%83%D0%BC%D0%B0) that speeds up selective queries by skipping data fragments in which the sought value is guaranteed to be absent. More information: [Bloom indexes](../dev/bloom-skip-indexes.md), [local indexes](query_execution/local_indexes.md).

#### Column family {#column-family}

**column family** or **column group** is a feature that allows you to store subsets of columns of a [row table](#row-oriented-table) separately in a distinct family or group. The primary use case is storing a portion of columns on different types of disks (moving less important columns to HDD) or with different compression settings. If your workload requires many column families, consider using [columnar tables](#column-oriented-table).

#### Column encoding {#column-encoding}

**column encoding** — it is a mechanism for optimizing the storage of data in table columns, which reduces the amount of disk space used and speeds up certain operations.

#### Lifetime {#ttl}

**time to live** or **TTL** — this is a mechanism for automatically deleting old rows from a table asynchronously in the background. It is described in a separate article [{#T}](ttl.md).

### Representation {#view}

**view** — is a way to save a query and treat its results as a real table. The view itself does not store data, except the query text. The query stored in the view is executed on each SELECT from it, generating the returned result. Any changes to the tables referenced by the view are immediately reflected in the read results from it.

{% if feature_view %}

Views can be user-defined or system-defined.

#### Custom views {#user-view}

**Custom views** are created by the user using the command [{#T}](../yql/reference/syntax/create-view.md). They are described in more detail in [{#T}](../concepts/datamodel/view.md).

{% endif %}

#### System views {#system-view}

**system views** — are special views automatically created by the system to monitor the state of the database and the cluster. They are located in the special directory `.sys`, which resides in the root folder of each database. System views for databases are described in [{#T}](../dev/system-views.md); system views for the cluster, as well as access‑control issues, are in [{#T}](../devops/observability/system-views.md).

### Topic {#topic}

**Message queue** is used for reliable asynchronous communication between different systems via message passing. {{ ydb-short-name }} provides infrastructure that ensures "exactly once" semantics in such communications. Using it, you can achieve guarantees of no lost messages and no accidental duplicates.

**topic** — a named entity in a message queue intended for interaction between [producers](#producer) and [consumers](#consumer).

Several terms related to topics are listed below. How topics {{ ydb-short-name }} work is explained in more detail in a separate article [{#T}](datamodel/topic.md).

#### Partition {#partition}

For horizontal scaling, topics are divided into separate elements called **partitions**. Thus, partitions are the unit of parallelism within a topic. Messages within each partition are ordered.

However, data subsets managed by a single [data shard](#data-shard) or [column shard](#column-shard) can also be called partitions.

#### Offset {#offset}

**offset** — the sequential number that identifies a message within a [partition](#partition).

#### Producer {#producer}

**producer** — an entity that writes new messages to a topic.

#### Consumer {#consumer}

**consumer** — an entity that reads messages from a topic.

### Change data capture {#cdc}

**change data capture**, **CDC** — a mechanism that allows you to subscribe to a **change stream** in a specific [table](#table). Technically it is implemented on top of [topics](#topic). It is described in more detail in a separate article [{#T}](cdc.md).

#### Change stream {#changefeed}

**change stream** — an ordered list of changes to a [table](#table) placed in a [topic](#topic).

### Backup collection {#backup-collection}

**backup collection** — a [schema object](#scheme-object) that organizes full and incremental [backups](#backup) for selected [row‑based tables](#row-oriented-table). Collections provide [point‑in‑time recovery](https://en.wikipedia.org/wiki/Point-in-time_recovery), maintain [backup chains](#backup-chain), and guarantee consistent restoration of multiple tables. A table can belong to only one backup collection at a time.

For more details, see [{#T}](datamodel/backup-collection.md).

#### Backup {#backup}

**backup** — a copy of data at a specific point in time that can be used for data restoration. In the context of [backup collections](#backup-collection), there are two types:

- **full backup**: a complete snapshot of all data in the collection. It serves as the basis for [backup chains](#backup-chain) and can be restored independently.
- **incremental backup**: captures only changes (inserts, updates, deletions) since the previous backup. It requires the entire backup chain for restoration.

#### Backup chain {#backup-chain}

**backup chain** — an ordered sequence of [backups](#backup) that starts with a full backup followed by zero or more incremental backups. Each incremental backup depends on all previous backups in the chain. Deleting any backup in the chain makes subsequent incremental backups unrecoverable.

{% if feature_async_replication == true %}

### Asynchronous replication instance {#async-replication-instance}

An **asynchronous replication instance** is a named entity that stores the settings of [asynchronous replication](async-replication.md) (connection settings, list of replicated objects, etc.). It can also be used to obtain information about the state of asynchronous replication: [initial scan progress](async-replication.md#initial-scan), [lag](async-replication.md#replication-of-changes), [errors](async-replication.md#error-handling), etc.

#### Replicated object {#replicated-object}

A **replicated object** is an object (for example, a table) for which asynchronous replication is configured.

#### Replica object {#replica-object}

A **replica object** is a "mirror copy" of the replicated object, automatically created by the asynchronous replication instance. Typically, it is read-only.

{% endif %}

{% if feature_transfer == true %}

### Transfer instance {#transfer-instance}

A **transfer instance** is a named entity that stores the settings of a [transfer](transfer.md), including connection settings and data transformation rules. It can also be used to obtain information about the transfer state, for example, [errors](transfer.md#error-handling).

{% endif %}

### Coordination node {#coordination-node}

A **coordination node** is a schema object that allows client applications to create semaphores to coordinate their actions. Coordination nodes are used to implement distributed locks, service discovery, leader election, and other scenarios. Learn more about [coordination nodes](./datamodel/coordination-node.md).

#### Semaphore {#semaphore}

A **semaphore** is an object inside a [coordination node](#coordination-node) that provides a synchronization mechanism for distributed applications. Semaphores can be persistent or temporary and support operations for creation, acquisition, release, and monitoring. Learn more about [semaphores in {{ ydb-short-name }}](./datamodel/coordination-node.md#semaphore).

{% if feature_resource_pool == true and feature_resource_pool_classifier == true %}

### Resource pool {#resource-pool}

A **resource pool** is a schema object that describes the limits imposed on resources (CPU, RAM, etc.) available for executing queries in this resource pool. A query is always executed in some resource pool. By default, all queries are executed in a resource pool named `default`, which does not impose any restrictions. For more information about using resource pools, see [{#T}](../dev/resource-consumption-management.md).

### Resource pool classifier {#resource-pool-classifier}

A **resource pool classifier** is an object designed to manage the distribution of queries among [resource pools](#resource-pool). It describes the rules by which a resource pool is selected for each query. These classifiers are global for the entire [database](#database) and apply to all queries entering it. For more information about their use, see [{#T}](../dev/resource-consumption-management.md).

{% endif %}

### YQL {#yql}

**YQL ({{ ydb-short-name }} Query Language)** is a high-level language for working with the system. It is a dialect of [ANSI SQL](https://en.wikipedia.org/wiki/SQL). There are many materials dedicated to YQL, including a [tutorial](../dev/yql-tutorial/index.md), [reference guide](../yql/reference/syntax/index.md), and [recipes](../yql/reference/recipes/index.md).

### Federated queries {#federated-queries}

**Federated queries** is a feature that allows you to execute queries against data stored in systems external to the {{ ydb-short-name }} cluster.

Below are explanations of several terms related to federated queries. How federated queries work in {{ ydb-short-name }} is explained in more detail in a separate article [{#T}](query_execution/federated_query/index.md).

#### External data source {#external-data-source}

An **external data source**, also known as an **external connection**, is metadata that describes how to connect to a supported external system to execute [federated queries](#federated-queries).

#### External table {#external-table}

An **external table** is metadata that describes a specific dataset that can be retrieved from an [external data source](#external-data-source).

#### Secret {#secret}

A **secret** is confidential metadata that requires special handling. For example, secrets can be used in definitions of [external data sources](#external-data-source) and represent entities such as passwords and tokens.

### Authentication token {#auth-token}

An **auth token** is a token used for [authentication](../security/authentication.md) in {{ ydb-short-name }}.

{{ ydb-short-name }} supports [different types of authentication](../security/authentication.md) and various token types.

### mTLS {#mtls}

**mTLS** (mutual TLS) — a TLS mode [TLS](https://ru.wikipedia.org/wiki/Transport_Layer_Security) in which not only the client verifies the server's certificate, but the server also requests and verifies the [client certificate](#client-certificate) during connection establishment.

### Client certificate {#client-certificate}

**client certificate** (or **certificate of the client**) — [digital certificate](https://en.wikipedia.org/wiki/X.509) issued and used by the client — an application, a user, or a [node {{ ydb-short-name }}](#node) — to prove its identity when interacting with {{ ydb-short-name }}.

### Cluster schema {#scheme}

**Cluster schema {{ ydb-short-name }}** — is a hierarchical namespace of the cluster {{ ydb-short-name }}. The top-level element of this namespace is the [cluster schema root](#scheme-root). Child elements of the cluster schema root are [databases](#database). Inside each database you can create an arbitrary hierarchy of [objects](#scheme-object) (tables, topics, etc.) using nested directories.

### Database schema {#scheme-database}

**Database schema** — is a subset of the cluster's hierarchical namespace that belongs to a database.

### Database root {#scheme-database-root}

**Database root** — is the path to a database in the cluster schema.

### Schema root {#scheme-root}

**Cluster schema root** — is the root element of the [namespace {{ ydb-short-name }}](datamodel/cluster-namespace.md), whose child elements are [databases](#database).

### Schema object {#scheme-object}

A database schema consists of **schema objects**, which can be databases, [tables](#table) (including [external tables](#external-table)), [topics](#topic), [directories](#folder), etc.

For organizational convenience, schema objects form a hierarchy using [directories](#folder).

### Folder {#folder}

As in file systems, a **folder**, **directory** is a container for [schema objects](#scheme-object).

Directories can contain subdirectories, and such nesting can be of arbitrary depth.

### Access object {#access-object}

**access object** during [authorization](../security/authorization.md) is an entity for which access rights and restrictions are configured. In {{ ydb-short-name }}, access objects are [schema objects](#scheme-object).

Each [schema object](#scheme-object) has an [owner](#access-owner) and a [list of rights](#access-control-list) on that object, granted to users and groups ([access subjects](#access-subject)).

### Access subject {#access-subject}

**access subject** is an entity that can access [access objects](#access-object) and perform certain actions in the system.

Access acquisition for these calls and actions depends on the configured [list of rights](#access-control-list) and the subject's [access level](#access-level).

An access subject can be a [user](#access-user) or a [group](#access-group).

### Access right {#access-right}

**[Access right](../security/authorization.md#right)** — an entity that represents permission for an [access subject](#access-subject) to perform a specific set of operations in the cluster or database on a particular [access object](#access-object).

### Access rights inheritance {#access-right-inheritance}

**Access rights inheritance** is a mechanism whereby [access rights](#access-right) granted on parent [access objects](#access-object) are inherited by child objects in the hierarchical structure of the database. This ensures that permissions granted at a higher level of the hierarchy apply to all lower levels unless they are [explicitly overridden](../reference/ydb-cli/commands/scheme-permissions.md#clear-inheritance).

### Access control list {#access-control-list}

**[Access control list](../security/authorization.md#right)**, **access control list**, or **ACL** — a list of all [rights](#access-right) granted to [access subjects](#access-subject) (users and groups) for a specific [access object](#access-object).

### Access level {#access-level}

**Access level** provides [access subjects](#access-subject) with additional capabilities when working with [schema objects](#scheme-object), as well as the ability to perform operations on the cluster as a whole. {{ ydb-short-name }} uses hierarchical access levels:

- Database
- Viewer
- Monitoring
- Administration.

An access level for a subject is configured using [access level lists](#access-level-list).

### Access level list {#access-level-list}

**Access level list**, or **permission list** — a list of [SID](#access-sid)s of [access subjects](#access-subject) that are granted a specific [access level](#access-level).

In {{ ydb-short-name }} there are [several such lists](../reference/configuration/security_config.md#security-access-levels) that determine who has which [access levels](#access-level).

Detailed information about access level lists, their hierarchy, and how they work is provided in the [Access level lists](../security/authorization.md#access-level-lists) section of the authorization documentation.

### Owner {#access-owner}

**[Owner](../security/authorization.md#owner)** - [access subject](#access-subject) ([user](#access-user) or [group](#access-group)) having full rights on a specific [access object](#access-object).

### User {#access-user}

**[User](../security/authorization.md#user)** - a person who uses {{ ydb-short-name }} to perform a specific function.

In {{ ydb-short-name }} there are different types of users depending on how they are created:

- Local users in {{ ydb-short-name }} databases.
- External users from external directories.

A user is identified by a [SID](#access-sid).

#### Local user {#local-user}

A user whose account is created directly in {{ ydb-short-name }} using the YQL command `CREATE USER` or during [initial security configuration](../security/builtin-security.md).

#### External user {#external-user}

A {{ ydb-short-name }} user whose account is created in an external directory, such as an LDAP directory or an IAM system.

### Group {#access-group}

**[Group](../security/authorization.md#group)** or **access group** - a named set of [users](#access-user) and other groups with equal privileges for its members.

A group is identified by a [SID](#access-sid).

### Role {#access-role}

A role is a named set of [access rights](#access-right) used to assign to [users](#access-user) or [groups](#access-group).

Roles in {{ ydb-short-name }} are implemented using [groups](#access-group) that are created during the initial cluster deployment and are assigned a specific [list of rights](#access-right) at the root of the cluster schema. For more about roles, see the article [{#T}](../security/builtin-security.md).

### SID {#access-sid}

**SID** or **security identifier** — a string of the form `<name>` or `<name>@<auth-domain>` that identifies an [access subject](../concepts/glossary.md#access-subject). It is used for [authentication](../security/authentication.md), [authorization](../security/authorization.md), in [rights lists](#access-control-list) and in [access level lists](#access-level-list).

SID identifies an individual [user](#access-user) or a [user group](#access-group).

An optional suffix `@<auth-domain>` identifies the source of the access subject, i.e., the external directory or system from which it was obtained. For example, users or groups from an LDAP directory may have the suffix `@ldap`. The absence of a suffix means that the user or group is created and exists directly in {{ ydb-short-name }}.

### Query optimizer {#optimizer}

[**Query optimizer**](https://ru.wikipedia.org/wiki/%D0%9E%D0%BF%D1%82%D0%B8%D0%BC%D0%B8%D0%B7%D0%B0%D1%86%D0%B8%D1%8F_%D0%B7%D0%B0%D0%BF%D1%80%D0%BE%D1%81%D0%BE%D0%B2_%D0%A1%D0%A3%D0%91%D0%94) — a set of {{ ydb-short-name }} components responsible for transforming a logical query representation into a concrete physically executable plan to obtain the requested result. The main goal of the optimizer is to choose, among all possible query execution plans, one that is sufficiently efficient in terms of predicted execution time and cluster resource consumption. It is described in more detail in a separate article [{#T}](query_execution/optimizer.md).

### Compile cache {#compile-cache}

**Compile cache** — a cache of compiled queries on each [node](#node) of the cluster. It is used to avoid recompilation: if the query text is already in the node's cache, additional compilation is not performed. See more in the [Query compile cache](../dev/system-views.md#compile-cache-queries) section.

## Advanced terminology {#advanced-terminology}

This section explains terms that are useful for [{{ ydb-short-name }} contributors](../contributor/index.md) and users who want to gain a deeper understanding of what happens inside the system.

### Actor implementation {#actor-implementation}

#### Actor system {#actor-system}

**Actor system** — a C++ library with [implementation](https://github.com/ydb-platform/ydb/tree/main/ydb/library/actors) of the [actor model](https://en.wikipedia.org/wiki/Actor_model) for the needs of {{ ydb-short-name }}.

#### Actor service {#actor-service}

**Actor service** — a [actor](#actor) that has a known name and usually runs as a single instance on a [node](#node).

#### ActorId {#actorid}

**ActorId** — a unique identifier of an actor or a [tablet](#tablet) in a [cluster](#cluster).

#### Actor system interconnect {#actor-system-interconnect}

**Actor system interconnect**, **interconnect** — an internal network layer of the [cluster](#cluster). All [actors](#actor) communicate with each other in the system via the interconnect.

#### Local {#local}

**Local** — a [actor service](#actor-service) that runs on every [node](#node). It directly manages [tablets](#tablet) on its node and interacts with [Hive](#hive). It registers with Hive and receives commands to start tablets.

### Tablet implementation {#tablet-implementation}

[**tablet**](#tablet) — it is a [actor](#actor) with persistent state. It includes a set of data for which this tablet is responsible, and a finite‑state machine that modifies the tablet's data (or state). A tablet is a fault‑tolerant entity because the tablet's data is stored in [distributed storage](#distributed-storage) that survives disk and node failures. The tablet automatically restarts on another [node](#node) in case of failure or overload of the previous one. Data in the tablet changes sequentially, as the system infrastructure guarantees that there is no more than one [tablet leader](#tablet-leader) through which tablet data changes are performed.

A tablet solves the same problem as the [Paxos](https://ru.wikipedia.org/wiki/%D0%90%D0%BB%D0%B3%D0%BE%D1%80%D0%B8%D1%82%D0%BC_%D0%9F%D0%B0%D0%BA%D1%81%D0%BE%D1%81) and [Raft](https://ru.wikipedia.org/wiki/Raft_(%D0%B0%D0%BB%D0%B3%D0%BE%D1%80%D0%B8%D1%82%D0%BC)) algorithms in other systems, namely the problem of [distributed consensus](https://ru.wikipedia.org/wiki/%D0%9A%D0%BE%D0%BD%D1%81%D0%B5%D0%BD%D1%81%D1%83%D1%81_%D0%B2_%D1%80%D0%B0%D1%81%D0%BF%D1%80%D0%B5%D0%B4%D0%B5%D0%BB%D1%91%D0%BD%D0%BD%D1%8B%D1%85_%D0%B2%D1%8B%D1%87%D0%B8%D1%81%D0%BB%D0%B5%D0%BD%D0%B8%D1%8F%D1%85). From a technical point of view, the tablet implementation can be described as a replicated state machine (Replicated State Machine, RSM) on top of a shared log, because the tablet state is fully described by an ordered command log stored in a distributed and fault‑tolerant storage.

During execution, the tablet's state machine is managed by three components:

1. The common **tablet** part ensures log consistency and recovery in case of failures.
2. **executor** — this is an abstraction of a local database, namely the data structures and code that organize work with data stored in a tablet.
3. An actor with user code that implements the specific logic of a particular tablet type.

In {{ ydb-short-name }} there are several types of specialized tablets that store all kinds of data for various tasks. Many {{ ydb-short-name }} functions, such as [tables](#table) and [topics](#topic), are implemented as different types of tablets. Thus, reusing the tablet infrastructure is one of the key means of extending {{ ydb-short-name }} as a platform.

Usually, in a cluster {{ ydb-short-name }}, there are orders of magnitude more tablets than the processes or threads that other systems would use for a cluster of comparable size. In a cluster {{ ydb-short-name }}, hundreds of thousands and millions of tablets can easily work simultaneously.

Since a tablet stores its state in [distributed storage](#distributed-storage), it can be (re)started on any cluster node. Tablets are identified by [TabletID](#tabletid), a 64‑bit number assigned when the tablet is created.

### Tablet leader {#tablet-leader}

**tablet leader** — this is the current active leader of a given tablet. The tablet leader receives commands, assigns them order, and confirms them to the external world. It is guaranteed that at any moment there is at most one leader for each tablet.

### Tablet candidate {#tablet-candidate}

**tablet candidate** — is one of the election participants that wants to become the [leader](#tablet-leader) of this tablet. If the candidate wins the election, it becomes the tablet's leader.

### Tablet replica {#tablet-follower}

**tablet replica**, **tablet follower**, or **hot standby** — is a copy of the [tablet leader](#tablet-leader) that applies the command log accepted by the leader (with some delay). A tablet can have zero or more replicas. Replicas perform two main functions:

* In the event of a leader’s termination or failure, replicas are preferred [candidates](#tablet-candidate) for the role of a new leader, as they can become leader much faster than other candidates because they have applied most of the log.
* Replicas can respond to read-only requests if the client explicitly selects an optional relaxed transaction mode that allows stale reads.

### Tablet generation {#tablet-generation}

**tablet generation** — the number that identifies the reincarnation of a tablet leader. It changes only when a new leader is selected and always increases.

### Local tablet database {#local-database}

**tablet local database**, or **local database** — is a set of data structures and associated code that manage a tablet's state and the data stored by it. Logically, the state of the local database is represented by a set of tables that closely resemble relational tables. The local database state is modified by the tablet's local transactions created by the tablet's user actor.

Each table of the local database is stored as an [LSM tree](#lsm-tree).

#### Log-structured merge-tree {#lsm-tree}

**[Log-structured merge-tree](https://ru.wikipedia.org/wiki/LSM-%D0%B4%D0%B5%D1%80%D0%B5%D0%B2%D0%BE)** or **LSM tree** — is a data structure designed to optimize write and read performance in storage systems. It is used in {{ ydb-short-name }} to store tables of the [local database](#local-database) and data of [VDisks](#vdisk).

#### MemTable {#memtable}

All data written to tables of the [local database](#local-database) are initially stored in an in‑memory data structure called **MemTable**. When a MemTable reaches the configured size, it is flushed to disk as an immutable data structure [SST](#sst).

#### Sorted string table {#sst}

**Sorted string table**, or **SST** — is an immutable data structure that stores table rows sorted by key, facilitating efficient key lookup and range queries. Each SST consists of a continuous series of small data pages, typically about 7 KiB each, which further optimizes disk read performance. An SST is usually part of an [LSM tree](#lsm-tree).

#### Tablet pipe {#tablet-pipe}

**tablet pipe**, or **TabletPipe** — is a virtual connection that can be established with a tablet. It includes locating the [tablet leader](#tablet-leader) via [TabletID](#tabletid). This is the recommended way to interact with a tablet. The term **open tablet pipe** describes the process of resolving (locating) a tablet in the cluster and establishing a virtual communication channel with it.

#### TabletID {#tabletid}

**TabletID** — is a unique identifier of a [tablet](#tablet) within a cluster.

#### Bootstrapper {#bootstrapper}

**Bootstrapper** — is the primary mechanism for starting tablets, used for system tablets (for example, for [Hive](#hive), [DS controller](#ds-controller), the root [SchemeShard](#scheme-shard)). [Hive](#hive) initializes the remaining tablets.

### Shared cache {#shared-cache}

**shared cache** — is an [actor](#actor) that stores data pages recently read from [distributed storage](#distributed-storage). Caching these pages reduces disk I/O operations and speeds up data retrieval, improving overall system performance.

### Memory controller {#memory-controller}

**memory controller** — is an [actor](#actor) that manages [memory limits](../reference/configuration/memory_controller_config.md) {{ ydb-short-name }}.

### Spilling {#spilling}

**spilling** — is a memory management mechanism in {{ ydb-short-name }} that temporarily offloads intermediate query data to external storage when such data exceeds the node's available RAM. In {{ ydb-short-name }}, disk is currently used for spilling.

For more details on spilling, see [{#T}](query_execution/spilling.md).

### Tablet types {#tablet-types}

[Tablets](#tablet) can be viewed as a framework for building reliable components that operate in a distributed system. Many {{ ydb-short-name }} components—both system and user‑data components—are implemented using this framework; the main ones are listed below.

#### SchemeShard {#scheme-shard}

**SchemeShard**, or **Scheme shard** — is a system tablet that stores the database schema, including metadata of user [tables](#table), [topics](#topic), etc.

In addition, there is a **root SchemeShard** that stores information about databases created in the cluster.

#### DataShard {#data-shard}

**DataShard** or **Data shard** is a tablet that manages a segment of a [row user table](datamodel/table.md#row-oriented-tables). A logical user table is divided into segments by continuous ranges of the table’s primary key. Each such range is handled by a separate DataShard tablet. The range itself is also called a [partition](#partition). The DataShard tablet stores data row‑by‑row, which is efficient for OLTP workloads.

#### ColumnShard {#column-shard}

**ColumnShard** or **Column shard** is a tablet that stores a data segment of a [columnar user table](datamodel/table.md#column-oriented-tables).

#### KeyValue Tablet {#kv-tablet}

**KeyValue**, **KV Tablet**, or **key‑value tablet** is a tablet that implements a simple key → value mapping where keys and values are strings. It also provides several specialized features such as locks.

#### PersQueue Tablet {#pq-tablet}

**PersQueue** or **persistent queue tablet** is a tablet that implements the concept of a [topic](#topic). Each topic consists of one or more partitions, and each partition is managed by a separate PQ tablet instance.

#### TxAllocator {#txallocator}

**TxAllocator**, or **transaction allocator**, is a system tablet that allocates unique transaction identifiers ( [TxID](#txid)) in the cluster. Typically, the cluster has several such tablets, from which the [transaction proxy](#transaction-proxy) pre‑allocates and caches ranges for local issuance within a single process.

#### Coordinator {#coordinator}

**Coordinator** is a system tablet that provides global transaction ordering. The coordinator’s task is to assign logical time [PlanStep](#planstep) to each transaction planned through it. Each transaction is assigned exactly one coordinator, selected by hashing its [TxId](#txid).

#### Mediator {#mediator}

**Mediator** is a system tablet that distributes transactions planned by [coordinators](#coordinator) among transaction participants. Mediators ensure the advancement of global time. Each transaction participant is associated with exactly one mediator. Mediators eliminate the need for a full mesh of connections between all coordinators and all participants of all transactions.

#### Hive {#hive}

**Hive** is a system tablet responsible for launching and managing other tablets. Its duties include moving tablets between **nodes** in case of failure or overload of a [node](#node).{% if audience != "corp" %} More details about Hive can be found in a [separate article](../contributor/hive.md).{% endif %}

#### CMS {#cms}

**CMS**, or **cluster management system**, is a system tablet responsible for managing information about the current state of the [cluster {{ ydb-short-name }}](#cluster). This information is used to perform rolling restarts of the cluster without affecting user workloads, maintenance, cluster reconfiguration, etc.

#### NodeBroker {#node-broker}

**NodeBroker** is a system tablet that handles the registration of [dynamic nodes](#dynamic) in the cluster.

#### BSController {#ds-controller}

**BSController**, or **blob storage controller**, manages the dynamic configuration of the distributed storage, including information about [PDisk](#pdisk), [VDisk](#vdisk) and [storage groups](#storage-group). It interacts with the [node warden](#node-warden) to launch various distributed storage components. It also interacts with [Hive](#hive) to allocate [channels](#channel) to tablets.

#### Console {#console}

**Console** is a system tablet responsible for storing [dynamic configuration](../devops/configuration-management/configuration-v1/dynamic-config.md) and delivering it to the cluster **nodes**.

#### Kesus {#kesus}

**Kesus** is a tablet that implements a [coordination node](datamodel/coordination-node.md).

#### SysViewProcessor {#sys-view-processor}

**SysViewProcessor** is a tablet that stores data of some [system views](../dev/system-views.md).

{% if feature_serial %}

#### SequenceShard {#sequence-shard}

**SequenceShard** is a tablet that serves Sequence objects, which are used to implement [serial data types](../yql/reference/types/serial.md).

{% endif %}

{% if feature_async_replication %}

#### ReplicationController {#replication-controller}

**ReplicationController** is a tablet responsible for the [asynchronous replication](async-replication.md) process.

{% endif %}

#### StatisticsAggregator {#statistics-aggregator}

**StatisticsAggregator** — is a tablet responsible for gathering statistics used in cost optimization.

### Slot {#slot}

**Slot** in {{ ydb-short-name }} can be used in two contexts:

* **Slot** — is a portion of server resources allocated for running a single [node](#node) {{ ydb-short-name }}. The typical slot size is 10 CPU cores and 50 GB of RAM. Slots are used when a cluster {{ ydb-short-name }} is deployed on servers or virtual machines with enough resources to host multiple slots.
* **VDisk Slot** or **VSlot** — is a portion of a [PDisk](#pdisk) that can be allocated to one of the [VDisk](#vdisk).

### State storage {#state-storage}

**state storage**, **StateStorage** — is a distributed service that stores information about tablets, namely:

* The current tablet leader or its absence.
* Tablet replicas.
* The tablet generation and step `(generation:step)`.

State storage is used as a service for resolving tablet names, i.e., for obtaining an [ActorId](#actorid) by a [TabletID](#tabletid). StateStorage is also used in the process of selecting a [tablet leader](#tablet-leader).

The information in state storage is volatile. Consequently, it is lost on power loss or process restart. Despite its name, this service is not a permanent long‑term storage. It contains only information that is easy to recover and does not need to be durable. However, state storage keeps information on multiple nodes to minimize the impact of node failures. Through this service you can also gather a quorum, which is used for selecting tablet leaders.

Because of its nature, the state storage service operates on a best‑effort basis. For example, the absence of several tablet leaders is guaranteed through the leader‑selection protocol on the [distributed storage](#distributed-storage), not on state storage.

More details about the design of StateStorage and related subsystems are in the [Metadata distribution services](architecture/metadata-services.md) section.

### Board {#board}

**Board** — a distributed service designed to store metadata as key‑value pairs. It is also used to store information about [endpoints](../concepts/connect.md#endpoint).

More details about the design of Board and related subsystems are in the [Metadata distribution services](architecture/metadata-services.md) section.

### SchemeBoard {#scheme-board}

**SchemeBoard** — a distributed service designed to store metadata as key‑value pairs. It is also used to store information about [schemas](#global-schema).

More details about the design of SchemeBoard and related subsystems are in the [Metadata distribution services](architecture/metadata-services.md) section.

#### Compaction {#compaction}

**compaction**, **VDisk compaction** or **tablet compaction** — is an internal background process that rebuilds data of an [LSM tree](#lsm-tree). Data in [VDisk](#vdisk) and [local databases](#local-database) are organized as LSM trees. Therefore, there are **VDisk compaction** and **tablet compaction**. The compaction process is usually quite resource‑intensive, so measures are taken to minimize its overhead, for example by limiting the number of concurrent compactions.

#### gRPC proxy {#grpc-proxy}

**gRPC proxy** — is a proxy system for external client requests. Client requests arrive in the system via the [gRPC](https://grpc.io) protocol, after which the proxy component translates them into internal calls to execute those requests, which are passed through the [interconnect](#actor-system-interconnect). This proxy provides an interface for both request‑response and bidirectional streaming.

### Distributed configuration {#distributed-configuration}

**Distributed configuration**, **DistConf** — is an internal mechanism for [cluster configuration](../devops/configuration-management/configuration-v2/config-overview.md) that provides startup and setup of [static nodes](#static-node), automatic management of the [static storage group](#static-group) and [State Storage](../concepts/glossary.md#state-storage). Distributed configuration starts before any [tablets](#tablet), [storage groups](#storage-group), and [State Storage](../concepts/glossary.md#state-storage) are started.

More details about the design of distributed configuration are described in [{#T}](../contributor/configuration-v2.md).

### Implementation of distributed storage {#distributed-storage-implementation}

**Distributed storage** is a distributed fault‑tolerant data storage layer that stores binary records called [LogoBlob](#logoblob), addressed by a specific identifier type called [LogoBlobID](#logoblobid). Thus, distributed storage is a key‑value store that maps a LogoBlobID to a string up to 10 MiB. Distributed storage consists of many [storage groups](#storage-group), each of which is an independent data repository.

Distributed storage stores immutable data, with each immutable data block identified by a specific LogoBlobID key. The distributed storage API is highly specific, intended only for use by [tablets](#tablet) to store their data and change logs. Therefore, it is not meant for general‑purpose data storage. Data in distributed storage are deleted using special barrier commands. Because its interface lacks mutations, distributed storage can be implemented without implementing [distributed consensus](https://ru.wikipedia.org/wiki/%D0%9A%D0%BE%D0%BD%D1%81%D0%B5%D0%BD%D1%81%D1%83%D1%81_%D0%B2_%D1%80%D0%B0%D1%81%D0%BF%D1%80%D0%B5%D0%B4%D0%B5%D0%BB%D1%91%D0%BD%D0%BD%D1%8B%D1%85_%D0%B2%D1%8B%D1%87%D0%B8%D1%81%D0%BB%D0%B5%D0%BD%D0%B8%D1%8F%D1%85). Distributed storage is only one of the components that tablets use to implement distributed consensus.

#### LogoBlob {#logoblob}

**LogoBlob** is a set of binary immutable data identified by [LogoBlobID](#logoblobid) and stored in [distributed storage](#distributed-storage). The data block size is limited at the [VDisk](#vdisk) level and above in the stack. Currently, the maximum data block size that VDisk can handle is 10 MiB.

#### LogoBlobID {#logoblobid}

**LogoBlobID** is an identifier of a [LogoBlob](#logoblob) in [distributed storage](#distributed-storage). It has a structure of the form `[TabletID, Generation, Step, Channel, Cookie, BlobSize, PartID]`. The main components of a LogoBlobID:

* `TabletID` is the [ID](#tabletid) of the tablet that owns the LogoBlob.
* `Generation` is the generation of the tablet in which the data block was written.
* `Channel` is the [channel](#channel) of the tablet on which the LogoBlob is written.
* `Step` is an incremental counter, usually within the tablet's generation.
* `Cookie` is a unique identifier of a data block within a single `Step`. A cookie is typically used when writing multiple data blocks into one `Step`.
* `BlobSize` is the size of the LogoBlob.
* `PartID` is the identifier of a part of a data block. It is important when the original LogoBlob is split into parts using [erasure coding](#erasure-coding), and the parts are written to the corresponding [VDisk](#vdisk) and [storage groups](#storage-group).

#### Replication {#replication}

**Replication** is a process that ensures a sufficient number of data copies (replicas) to maintain the desired availability characteristics of a cluster {{ ydb-short-name }}. It is typically used in geo‑distributed clusters {{ ydb-short-name }}.

#### Erasure coding {#erasure-coding}

[**Erasure coding**](https://ru.wikipedia.org/wiki/%D0%A1%D1%82%D0%B8%D1%80%D0%B0%D1%8E%D1%89%D0%B8%D0%B9_%D0%BA%D0%BE%D0%B4) is a data encoding method in which the original data are supplemented with redundancy and split into multiple fragments, allowing reconstruction of the original data if one or more fragments are lost. It is widely used in clusters {{ ydb-short-name }} with a single [availability zone](#regions-az), unlike [replication](#replication) with three replicas. For example, the popular 4+2 erasure coding scheme provides the same reliability as three replicas, with a storage overhead of 1.5 versus 3.

#### PDisk {#pdisk}

**PDisk**, **physical disk** is a component that controls a physical disk storage (block device). In other words, PDisk is a subsystem that implements an abstraction similar to a specialized file system on top of block devices (or files emulating a block device for testing purposes). PDisk provides data integrity checks (including [erasure coding](#erasure-coding) of sector groups for recovering data on individual damaged sectors, integrity verification via checksums), transparent encryption of all data on the disk, and transactional guarantees for disk operations (write confirmation strictly after `fsync`).

PDisk contains a scheduler that ensures sharing of the device's bandwidth among multiple clients ( [VDisk](#vdisk)). PDisk divides the block device into blocks called [slots](#slot) (about 128 megabytes in size; smaller blocks are also allowed). At any given time, no more than one VDisk can own each slot. PDisk also maintains a recovery log shared by the PDisk service records and all VDisks.

#### VDisk {#vdisk}

**VDisk** or **virtual disk** is a component that implements data storage of the [distributed storage](#distributed-storage) [LogoBlob](#logoblob) on a [PDisk](#pdisk). A VDisk stores all its data on a PDisk. One VDisk corresponds to one PDisk, but typically multiple VDisks are associated with a single PDisk. Unlike PDisk, which hides blocks and logs, VDisk provides an interface at the LogoBlob and [LogoBlobID](#logoblobid) level, such as writing a LogoBlob, reading LogoBlobID data, and deleting a set of LogoBlobs using a special command. A VDisk is a member of a [storage group](#storage-group). The VDisk itself is local, but many VDisks in a given group provide reliable data storage. VDisks in a group synchronize data with each other and replicate data in case of loss. The set of VDisks in a storage group forms a distributed RAID.

#### Yard {#yard}

**Yard** is the name of the [PDisk](#pdisk) API. It allows [VDisk](#vdisk) to read and write data to blocks and logs, reserve blocks, delete blocks, and transactionally acquire and release block ownership. In some contexts, Yard can be considered synonymous with PDisk.

#### Skeleton {#skeleton}

**Skeleton** is an [actor](#actor) that provides an interface to [VDisk](#vdisk).

#### SkeletonFront {#skeletonfront}

**SkeletonFront** is a proxy actor for Skeleton that controls the flow of messages coming into Skeleton.

#### Proxy {#ds-proxy}

**Distributed storage proxy**, **DS proxy**, **BS proxy**, **DS-proxy**, or **BS-proxy** acts as a client library for performing operations with [distributed storage](#distributed-storage). DS proxy users are [tablets](#tablet) that write to and read from distributed storage. The DS proxy hides the distributed nature of distributed storage from the user. The task of the DS proxy is to write to a quorum of [VDisks](#vdisk), perform retries when necessary, and control the write/read flow to prevent VDisk overload.

Technically, the DS proxy is implemented as an [actor service](#actor-service) launched by [node warden](#node-warden) on each node for each storage group, handling all requests to the group (writing, reading, and deleting [LogoBlobs](#logoblob), locking the group). When writing data, the DS proxy performs [erasure coding](#erasure-coding) of the data, splitting the LogoBlob into parts that are then sent to the corresponding VDisks. The DS proxy performs the reverse process when reading, receiving parts from VDisks and reconstructing the LogoBlob from them.

#### Node warden {#node-warden}

**Node warden** or `BS_NODE` — is an [actor service](#actor-service) on each cluster node, launching [PDisks](#pdisk), [VDisks](#vdisk) and [DS proxy](#ds-proxy) of [static storage groups](#static-group) when the node starts. It also interacts with the [DS controller](#ds-controller) to start PDisk, VDisk and DS proxy for [dynamic groups](#dynamic-group). The DS proxy for dynamic groups is started on demand: node warden processes “undelivered” messages to the DS proxy, starts the appropriate DS proxies, and obtains the group configuration from the DS controller.

#### Fail realm {#fail-realm}

**fail realm** — is a set of [failure domains](#fail-domain) that can fail simultaneously for a common cause. A correlated failure of two [VDisks](#vdisk) in the same fail realm is more likely than a failure of two VDisks from different fail realms.

An example of a failure domain is a set of equipment located in a single [data center or availability zone](#regions-az) that can fail entirely due to a natural disaster, a large‑scale power outage, or another similar event.

#### Fail domain {#fail-domain}

**fail domain** is a set of equipment that can fail simultaneously. A correlated failure of two [VDisk](#vdisk) within the same fail domain is more likely than the failure of two VDisk from different fail domains. In the case of different fail domains, the probability of a simultaneous failure also depends on whether the considered domains belong to the same failure area or to different ones.

An example of a failure domain is a set of disks attached to a single server, because all disks of a particular server can become unavailable if the server’s power supply or network controller fails. Typically, the common failure domain includes all servers located in the same [server rack](#rack), because power or network connection problems at the rack level cause the unavailability of all equipment hosted in it. Thus, a typical failure domain corresponds to a server rack (if the [cluster](#cluster) is configured with the rack placement topology in mind) or to a single server.

Failure domain-level failures are automatically handled {{ ydb-short-name }} without stopping the cluster.

#### Distributed storage channel {#channel}

A **distributed storage channel**, **DS channel**, or **channel** — is a logical connection between [tablet](#tablet) and a group of [distributed storage](#distributed-storage). A tablet can write data to various channels, and each channel maps to a specific [storage group](#storage-group). Having multiple channels allows the tablet to:

* Write more data than a single storage group can contain.
* Store different [LogoBlob](#logoblob) objects in different storage groups, with various properties such as error‑correcting encoding or on different media (HDD, SSD, NVMe).

### Implementation of distributed transactions {#transaction-implementation}

Below are the terms related to the implementation of [distributed transactions](#transactions).{% if oss == true %} The implementation itself is described in a separate article [{#T}](../contributor/datashard-distributed-txs.md).{% endif %}

#### Deterministic transactions {#deterministic-transactions}

Distributed transactions {{ ydb-short-name }} are inspired by the research work [Building Deterministic Transaction Processing Systems without Deterministic Thread Scheduling](http://cs-www.cs.yale.edu/homes/dna/papers/transactions-wodet11.pdf) by Alexander Thomson and Daniel J. Abadi from Yale University. The paper introduced the concept **deterministic transaction processing**, which enables efficient handling of distributed transactions. The original paper imposed restrictions on the types of operations that could be performed in this way. Because these restrictions interfered with real user scenarios, {{ ydb-short-name }} developed its own algorithms to execute them, using deterministic transactions as execution stages of user transactions with additional orchestration and locks.

#### Optimistic locks {#optimistic-locking}

As in many other database management systems, queries {{ ydb-short-name }} can place locks on certain data fragments, such as table rows, to ensure that concurrent modifications do not leave them in an inconsistent state. However, {{ ydb-short-name }} checks these locks not at the start of transactions but when attempting to commit them. The first approach is called **pessimistic locking** (for example, it is used in PostgreSQL), and the second is **optimistic locking** (used in {{ ydb-short-name }}).

#### Transaction lock invalidation {#tli}

**Transaction Lock Invalidation** (**TLI**) is a normal behavior of {{ ydb-short-name }} when parallel transactions conflict within [optimistic locks](#optimistic-locking). If one transaction (the violator) writes data and thereby breaks the locks of another transaction (the victim), {{ ydb-short-name }} detects this when the victim commits and aborts it with the `transaction locks invalidated` error. For more details on TLI diagnostics, see [{#T}](../troubleshooting/performance/queries/transaction-lock-invalidation.md).

#### Prepare phase {#prepare-stage}

The **prepare phase** is a transaction phase during which the transaction body is registered on all participating shards.

#### Execute phase {#execute-stage}

The **execute phase** is a transaction phase during which the scheduled transaction is executed and a response is generated.

In some cases, instead of [preparation](#prepare-stage) and execution, the transaction is executed immediately and a response is generated. For example, this happens for transactions that affect only one shard or for consistent reads from a data snapshot.

#### Dirty operations {#dirty-operations}

For read-only transactions, similar to "read uncommitted" in other database management systems, it may be necessary to read data that has not yet been committed to disk. This is called **dirty operations**.

#### Read-write set {#rw-set}

A **read-write set**, **RW-set** is a set of data that will participate in the execution of a [distributed transaction](#transactions). It combines the read set data that will be read and the write set for which modifications will be made.

#### Read set {#read-set}

A **read set**, **ReadSet data** is what participating shards send during transaction execution. For data transactions, it may contain information about the state of [optimistic locks](#optimistic-locking), the shard's readiness to commit, or a decision to abort the transaction.

#### Transaction proxies {#transaction-proxy}

A **transaction proxy** or `TX_PROXY` is a service that orchestrates the execution of many [distributed transactions](#transactions): sequential phases, phase execution, scheduling, and result aggregation. In the case of direct orchestration by other actors (for example, QP data transactions), it is used for caching and allocating unique [TxIDs](#txid).

#### Transaction flags {#txflags}

**Transaction flags** or **TxFlags** are a bitmask of flags that modify the transaction execution in some way.

#### Transaction ID {#txid}

A **transaction ID** or **TxID** is a unique identifier assigned to each transaction when it is accepted by {{ ydb-short-name }}.

#### Transaction order ID {#transaction-order-id}

A **transaction order ID** is a unique identifier assigned to each transaction during scheduling. It consists of [PlanStep](#planstep) and [Transaction ID](#txid).

#### Plan step {#planstep}

A **plan step**, **step**, **PlanStep**, or **Step** is the logical time at which a set of transactions is scheduled to execute.

#### Mediator time {#mediator-time}

During the execution of distributed transactions, **mediator time** is the logical time up to which (inclusive) a participating shard must know the entire execution plan. It is used to advance time when there are no transactions on a particular shard, to determine whether it can read from a snapshot.

#### MiniKQL {#minikql}

**MiniKQL** is a language that allows expressing a single [deterministic transaction](#deterministic-transactions) in the system. It is a functional, strictly typed language. Conceptually, the language describes a graph of reading from the database, performing computations on the read data, and writing results to the database and/or to a special document representing the query result (for display to the user). A MiniKQL transaction must explicitly specify its read set (the data to be read) and assume deterministic selection of execution branches (for example, no randomness).

MiniKQL is a low‑level language. End users of the system see only queries in the [YQL](#yql) language, which relies on MiniKQL in its implementation.

#### Query Processor {#kqp}

**Query Processor**, also known as **QP** (formerly **KQP**), is a {{ ydb-short-name }} component responsible for orchestrating the execution of user queries and generating the final response.

### Global schema {#global-schema}

**global scheme**, **global schema**, or **database schema** is the schema of all data stored in the [database](#database). It consists of [tables](#table) and other entities such as [topics](#topic). Metadata about these entities is called the global schema. The term is used in contrast to **local schema**, which refers to the data schema inside a [tablet](#tablet). Users of {{ ydb-short-name }} never see the local schema and work only with the global schema.

### KiKiMR {#kikimr}

**KiKiMR** is a legacy name for {{ ydb-short-name }} that was used before it became an [open‑source product](https://github.com/ydb-platform/ydb). It may still appear in source code, old articles, videos, etc.
