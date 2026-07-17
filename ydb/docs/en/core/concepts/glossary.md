# Glossary {{ ydb-short-name }}

This article provides an overview of the terms and definitions used in {{ ydb-short-name }} and its documentation. It [starts with key terms](#key-terminology) that are useful to get familiar with early in your work with {{ ydb-short-name }}, while the rest of the article contains [more advanced terms](#advanced-terminology) that may be helpful later.

## Key terminology {#key-terminology}

This section describes terms that are useful to anyone working with {{ ydb-short-name }}, regardless of their role or usage scenario.

### Cluster {#cluster}

A **cluster** {{ ydb-short-name }} is a set of interconnected [nodes](#node) {{ ydb-short-name }} that exchange data to execute user queries and ensure reliable data storage. These nodes form one of the supported [cluster topologies](#topology), which directly affects its reliability and performance characteristics.

Clusters {{ ydb-short-name }} are multi-tenant and can contain multiple isolated [databases](#database).

### Database {#database}

As in most database management systems, a **database** in {{ ydb-short-name }} is a logical container for other entities, such as [tables](#table). However, in {{ ydb-short-name }}, the namespace within databases is hierarchical, like in [virtual file systems](https://en.wikipedia.org/wiki/Virtual_file_system), and thus [directories](#folder) allow for a more structured organization of entities.

Another important characteristic of {{ ydb-short-name }} databases is that they are usually allocated dedicated computing resources. As a result, creating a database requires additional actions from [DevOps engineers](../devops/index.md).

### Node {#node}

{{ ydb-short-name }} A **node** is a server process that runs an executable file called `ydbd`. Multiple nodes {{ ydb-short-name }} can run on a single physical server or virtual machine, which is common practice. Thus, in the context of {{ ydb-short-name }}, nodes are **not** synonymous with hosts.

Since {{ ydb-short-name }} uses a storage and compute separation approach, `ydbd` has several operating modes that determine the node type. The available node types are described below.

#### Database node {#database-node}

**Database nodes** (also known as **tenant nodes** or **compute nodes**) process user queries addressed to a specific logical [database](#database). Their state is only in RAM and can be restored from [distributed storage](#distributed-storage). The set of database nodes of a given [cluster {{ ydb-short-name }}](topology.md) can be considered the compute layer of that cluster. Thus, adding database nodes and allocating additional resources (CPU and RAM) to them are the main ways to increase the compute resources of a database.

The main role of database nodes is to run various [tablets](#tablet) and [actors](#actor), as well as to receive incoming requests over the network.

#### Storage node {#storage-node}

**Storage nodes** are stateful nodes responsible for long-term storage of data fragments. The set of storage nodes in a given [cluster {{ ydb-short-name }}](#cluster) is called [distributed storage](#distributed-storage) and can be considered as the storage layer of that cluster. Thus, adding more storage nodes and their disks is the primary way to increase the storage capacity and I/O throughput of the cluster.

#### Hybrid node {#hybrid-mode}

A **hybrid node** is a process that simultaneously performs both the role of a [database node](#database-node) and a [storage node](#storage-node). Hybrid nodes are often used for development purposes. For example, you can run a container with a full-featured {{ ydb-short-name }} containing only one `ydbd` process in hybrid mode. They are rarely used in production environments.

#### Static node {#static-node}

**Static nodes** are configured manually during the initial cluster initialization or reconfiguration. As a rule, they serve as [storage nodes](#storage-node), but technically they can also be configured as [database nodes](#database-node).

#### Dynamic node {#dynamic}

**Dynamic nodes** are added to and removed from the cluster on the fly. They can only serve as [database nodes](#database-node).

### Distributed Storage {#distributed-storage}

**Distributed Storage**, **Distributed storage**, **Blob storage**, or **BlobStorage** is a distributed fault-tolerant data storage layer in {{ ydb-short-name }}. It has a specialized API designed for storing immutable data fragments of a [tablet](#tablet).

Many terms related to the [implementation of distributed storage](#distributed-storage-implementation) are discussed below.

### Storage Group {#storage-group}

A **storage group** is a place for reliable data storage, similar to [RAID](https://en.wikipedia.org/wiki/RAID), but using disks from multiple servers. Depending on the selected [cluster topology](#topology), storage groups use different algorithms to ensure high availability, similar to [standard RAID levels](https://en.wikipedia.org/wiki/Standard_RAID_levels).

[Distributed Storage](#distributed-storage) typically manages a large number of relatively small storage groups. Each group can be assigned to a specific [database](#database) to increase the disk space capacity and I/O throughput available to that database.

[Static](#static-group) and [dynamic](#dynamic-group) storage groups are physical, meaning their data is placed directly on [VDisk](#vdisk)s.

#### Static Group {#static-group}

A **static group** is a special [storage group](#storage-group) created during the initial cluster deployment. Its main role is to store data of system [tablets](#tablet), which can be considered as cluster-level metadata.

The static group may require special attention during major cluster maintenance, such as decommissioning an [availability zone](#regions-az).

#### Dynamic Group {#dynamic-group}

Regular storage groups that are not [static](#static-group) are called **dynamic groups** or **dynamic group**. They are called dynamic because they can be created and deleted on the fly while the [cluster](#cluster) is running.

#### Virtual Storage Group {#virtual-storage-groups}

A **virtual storage group** is an entity that is not actually a [storage group](#storage-group) but appears as one from the outside (provides a similar external interface). It can store its data in other storage groups or in S3.

### Storage Pool {#storage-pool}

A **storage pool** is a set of data storage devices with similar characteristics. Each storage pool is assigned a unique name within the {{ ydb-short-name }} cluster. Technically, each storage pool consists of many physical disks ( [PDisk](#pdisk)). Each [storage group](#storage-group) is created in a specific storage pool, which determines the performance characteristics of the storage group through the selection of appropriate storage devices. Typically, separate storage pools are created for devices of different types (for example, NVMe, SSD, and HDD) or for specific models of these devices that have different capacities and access speeds.

### Actor {#actor}

The [actor model](https://ru.wikipedia.org/wiki/%D0%9C%D0%BE%D0%B4%D0%B5%D0%BB%D1%8C_%D0%B0%D0%BA%D1%82%D0%BE%D1%80%D0%BE%D0%B2) is one of the main approaches to concurrency used in {{ ydb-short-name }}. In this model, **actors** or **actor** are lightweight user-space processes that can have and modify their private state but can only affect each other indirectly through message passing. {{ ydb-short-name }} has its own implementation of this model, which is described [below](#actor-implementation).

In {{ ydb-short-name }}, actors with reliably persisted state are called [tablets](#tablet).

### Tablet {#tablet}

A **tablet** is one of the main building blocks and abstractions of {{ ydb-short-name }}. It represents an entity responsible for a relatively small segment of user or system data. Typically, a tablet manages up to several gigabytes of data, although some types of tablets can handle larger volumes.

For example, a [string user table](#row-oriented-table) is managed by one or more tablets of the [DataShard](#data-shard) type, with each tablet responsible for a continuous range of [primary keys](#primary-key) and their corresponding data.

End users sending queries to the {{ ydb-short-name }} cluster for execution do not need to know the details about tablets, their types, or operating principles, but this knowledge can be useful, for example, for performance optimization.

Technically, tablets are [actors](#actor) with state reliably stored in [distributed storage](#distributed-storage). This state allows the tablet to continue operating on a different [database node](#database-node) if the previous one fails or is overloaded.

[Tablet implementation details](#tablet-implementation) and related terms, as well as [basic tablet types](#tablet-types), are discussed below.

### Transactions {#transactions}

{{ ydb-short-name }} implements **transactions** at two main levels:

* [Local database](#local-database) and the rest of the [tablet infrastructure](#tablet-implementation) allow [tablets](#tablet) to manipulate their state using **local transactions** with [serializable isolation level](https://ru.wikipedia.org/wiki/%D0%A3%D1%80%D0%BE%D0%B2%D0%B5%D0%BD%D1%8C_%D0%B8%D0%B7%D0%BE%D0%BB%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%BD%D0%BE%D1%81%D1%82%D0%B8_%D1%82%D1%80%D0%B0%D0%BD%D0%B7%D0%B0%D0%BA%D1%86%D0%B8%D0%B9#Serializable_(%D1%83%D0%BF%D0%BE%D1%80%D1%8F%D0%B4%D0%BE%D1%87%D0%B8%D0%B2%D0%B0%D0%B5%D0%BC%D0%BE%D1%81%D1%82%D1%8C)). Technically, they are not local to a single node, as this state is stored remotely in [distributed storage](#distributed-storage).
* In the context of {{ ydb-short-name }}, the term **distributed transactions** usually refers to transactions spanning multiple tablets. For example, transactions between tables or even rows of a single table are often distributed.
* **Single-shard** transactions span a single tablet and execute faster. For example, transactions between rows of a single table partition are often single-shard.

These mechanisms allow {{ ydb-short-name }} to provide [strong consistency](https://en.wikipedia.org/wiki/Consistency_model#Strict_consistency).

{% if oss %}

The implementation of distributed transactions is discussed in a separate article [{#T}](../contributor/datashard-distributed-txs.md), and a list of several [related terms](#deterministic-transactions) is provided below.

{% endif %}

### Sessions

Logical connections to the database that store the context required for executing queries and managing transactions. Sessions are described in more detail in the section {#T}.

### Client-side timeout {#client-timeout}

**Client-side timeout** is a time limit that the application or {{ ydb-short-name }} SDK waits for a database operation to complete (for example, executing a query or receiving a response to a gRPC call). Upon expiration of this time, the client usually aborts the wait: closes the connection or data stream, receives a transport error or an error from the SDK — even before the server has returned an explicit response (see {{ ydb-short-name }} server [response codes](../reference/ydb-sdk/ydb-status-codes.md)).

If the client-side timeout is shorter than the execution duration of the query on the {{ ydb-short-name }} side, due to the specifics of query processing in the cluster, a query aborted on the client side may continue to execute on the server for some time. If such a situation occurs on a large scale, the server becomes overloaded with queries for which the client is not waiting for a response. Therefore, frequent retries of the same query immediately after a timeout can exacerbate the overload. For more details, see the articles [{#T}](../troubleshooting/performance/queries/retry-cascade.md) and [{#T}](../troubleshooting/performance/queries/overloaded-errors.md); retry policies in the SDK are described in the section [{#T}](../reference/ydb-sdk/error_handling.md).

### Transaction retry {#transaction-retry}

**Transaction retry** is a client-side practice of re-executing a [transaction](#transactions) entirely from the beginning upon a retryable error (for example, a temporary network failure or an optimistic locking conflict). In {{ ydb-short-name }}, retries should be performed at the transaction level, not at the level of individual queries within it. Built-in retry policies in the {{ ydb-short-name }} SDK and integrations (for example, spring-ydb-retry) implement this approach. For more details, see [{#T}](../reference/ydb-sdk/error_handling.md).

### Exponential backoff {#exponential-backoff}

**Exponential backoff** (also known as **backoff**) is a pause strategy between [transaction retry](#transaction-retry) attempts: the wait interval increases exponentially with each attempt, usually with an upper limit. The {{ ydb-short-name }} SDK often uses two levels of backoff — fast and slow — depending on the error type. For more details, see [{#T}](../reference/ydb-sdk/error_handling.md#handling-retryable-errors).

### Jitter {#jitter}

**Jitter** is a small random variation added to the delays of [exponential backoff](#exponential-backoff). It helps prevent many clients from retrying simultaneously after a common failure (a "retry storm") and distributes the load more evenly.

### Idempotency {#idempotency}

**Idempotency** is a property of an operation: repeated execution has the same effect as a single execution (for example, `UPSERT` with a deterministic primary key or read operations). [Transaction retries](#transaction-retry) are safe only for idempotent operations or for retry errors where the server guarantees that the transaction was not committed. SDKs and client libraries {{ ydb-short-name }} can extend the set of retryable status codes if the calling code marks the operation as idempotent.

### Transaction interceptor {#transaction-interceptor}

A **transaction interceptor** is a Spring Framework component that wraps methods annotated with `@Transactional` and manages transaction boundaries. Modules like spring-ydb-retry replace the standard Spring interceptor, adding [transaction retry](#transaction-retry) logic around transactional methods.

### Implicit transactions {#implicit-transactions}

An **implicit transaction** is a query execution mode where the [transaction mode](transactions.md#modes) is not specified. In this case, {{ ydb-short-name }} independently determines whether to wrap them in a transaction. This mode is described in more detail in [{#T}](transactions.md#implicit).

### Multi-version concurrency control {#mvcc}

[**Multi-version concurrency control**](https://ru.wikipedia.org/wiki/MVCC), or **MVCC**, is a method used by {{ ydb-short-name }} to allow multiple concurrent transactions to access the database simultaneously without interfering with each other. It is described in more detail in a separate article [{#T}](query_execution/mvcc.md).

### Streaming queries {#streaming-query}

A type of query designed for [stream processing](https://en.wikipedia.org/wiki/Stream_processing) of an unbounded data stream. Unlike regular queries, streaming queries have no execution time limits, automatically restart on errors, and periodically save their state as [checkpoints](#streaming-queries-checkpoints) for fault tolerance. [Watermarks](#streaming-queries-watermarks) are used to track processing progress by event time.

Streaming queries are described in more detail in a separate article [{#T}](./streaming-query/streaming-query.md).

### Streaming query checkpoints {#streaming-queries-checkpoints}

A periodically saved state of a [streaming query](#streaming-query), necessary for automatically restoring its operation after failures in a distributed system. For more details about checkpoints, see the article [{#T}](../dev/streaming-query/checkpoints.md).

### Streaming query watermarks {#streaming-queries-watermarks}

A monotonically increasing lower bound on the event times in a [streaming query](#streaming-query) that may still arrive in the stream. When the watermark reaches a value X, the system declares that all events with a time less than X have most likely been received. For more details about watermarks, see the article [{#T}](./streaming-query/watermarks.md).

### Topology {#topology}

{{ ydb-short-name }} supports several [cluster](#cluster) **topologies**, described in more detail in a separate article [{#T}](topology.md). Several related terms are explained below.

#### Availability zones and regions {#regions-az}

An **availability zone** is a data center or its isolated segment with minimal physical distance between nodes and minimal risk of failure simultaneously with other availability zones. Thus, availability zones should not share common infrastructure such as power, cooling, or external network connections.

A **region** is a large geographic area containing multiple availability zones. The distance between availability zones in one region should be about 500 km or less. {{ ydb-short-name }} writes data to each availability zone in the region synchronously, ensuring reasonable latency and uninterrupted operation in the event of a failure of one of the availability zones.

#### Rack {#rack}

A **rack** or **server rack** is equipment used to organize the placement of multiple servers. Servers in the same rack are more likely to become unavailable simultaneously due to rack-level issues related to power supply, cooling, etc. {{ ydb-short-name }} can take into account information about which server is in which rack when placing each data fragment in environments based on physical servers.

#### Pile {#pile}

A **pile** is a set of nodes that can fail or be shut down simultaneously while other parts of the cluster remain operational. A pile can remain operational when other cluster nodes are shut down. Piles are used in [bridge mode](#bridge) to split a cluster into several parts between which synchronous replication is performed. A pile can consist of nodes from one or more regions.

#### Bridge mode {#bridge}

**Bridge mode** is a special cluster topology where data is stored with synchronous replication between multiple [piles](#pile). The specifics of this mode are described in [{#T}](topology.md#bridge) and [{#T}](bridge.md).

### Table {#table}

A **table** is a structured piece of information organized into rows and columns. Each row represents a single record or item, and each column represents a specific attribute or field with a defined data type.

There are two main approaches to representing tabular data in memory or on disks: [row-oriented (row by row)](#row-oriented-table) and [column-oriented (column by column)](#column-oriented-table). The chosen approach greatly affects the performance characteristics of operations on this data: the former is better suited for transactional workloads (OLTP), while the latter is better for analytical workloads (OLAP). {{ ydb-short-name }} supports both approaches.

#### Row-oriented table {#row-oriented-table}

**Row-oriented tables** store data for all or most columns of each row physically close together. They are described in more detail in [{#T}](datamodel/table.md#row-oriented-tables).

#### Column-oriented table {#column-oriented-table}

**Column-oriented tables** or **columnar tables** store data for each column separately. They are optimized for building aggregates over a small number of columns but are less suitable for accessing specific rows, as rows need to be reconstructed from their cells on the fly. They are described in more detail in [{#T}](datamodel/table.md#column-oriented-tables).

#### Primary key {#primary-key}

A **primary key** is an ordered list of columns whose values uniquely identify a row. It is used to create the table's [primary index](#primary-index). It is defined by the {{ ydb-short-name }} user when [creating a table](../yql/reference/syntax/create_table/index.md) and significantly affects the performance of operations on that table.

Guidance on choosing primary keys is provided in [{#T}](../dev/primary-key/index.md).

#### Primary index {#primary-index}

A **primary index** or **primary key index** is the main data structure used to find rows in a table. It is created based on the chosen [primary key](#primary-key) and determines the physical order of rows in the table; thus, each table can have only one primary index. The primary index is unique.

#### Secondary index {#secondary-index}

A **secondary index** is an additional data structure used to find rows in a table, typically when this cannot be done efficiently using the [primary index](#primary-index). Unlike the primary index, secondary indexes are managed independently of the table's main data. Thus, a table can have multiple secondary indexes for different scenarios. {{ ydb-short-name }} capabilities regarding secondary indexes are described in a separate article [{#T}](query_execution/secondary_indexes.md). A secondary index can be either unique or non-unique.

Special types of secondary indexes include the [vector index](#vector-index), [full-text index](#fulltext-index), and [JSON index](#json-index).

#### Vector index {#vector-index}

A **vector index** is an additional data structure used to speed up [vector search](query_execution/vector_search.md) when there is a large amount of data and [exact vector search without an index](../yql/reference/udf/list/knn.md) does not perform satisfactorily.
The capabilities of {{ ydb-short-name }} for approximate nearest neighbor search (ANN search) using vector indexes are described in a separate article [{#T}](../dev/vector-indexes.md).

A **vector index** is a specialized type of [secondary index](#secondary-index) designed for similarity search, unlike traditional secondary indexes that are optimized for equality or range lookups.

#### Full-text index {#fulltext-index}

A **full-text index** is an additional data structure used to speed up text search on a table column (by words and phrases, and when using N-grams, also by substrings).

Full-text search capabilities and index parameters are described in the articles [{#T}](../dev/fulltext-indexes.md) and [{#T}](query_execution/fulltext_search.md).

#### JSON index {#json-index}

A **JSON index** is an additional data structure used to accelerate predicates with the functions [JSON_EXISTS](../yql/reference/builtins/json.md#json_exists) and [JSON_VALUE](../yql/reference/builtins/json.md#json_value) on a column of type `Json` or `JsonDocument`. Unlike traditional secondary indexes, which are optimized for equality or range searches on individual table columns, a JSON index works with arbitrary [JsonPath](../yql/reference/builtins/json.md#jsonpath) paths inside a JSON document.

A JSON index, like a [full-text index](#fulltext-index), is implemented on top of an [inverted index](https://ru.wikipedia.org/wiki/%D0%98%D0%BD%D0%B2%D0%B5%D1%80%D1%82%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%BD%D1%8B%D0%B9_%D0%B8%D0%BD%D0%B4%D0%B5%D0%BA%D1%81), but uses its own JSON document tokenizer. JSON search capabilities are described in the articles {#T} and {#T}.

#### Local index {#local-index}

A local index is an auxiliary structure that is stored together with the table data (unlike a [global secondary index](#secondary-index), which materializes a separate index table). A local index is used when reading the main table on the storage side. For more information, see [local indexes](query_execution/local_indexes.md).

#### Bloom filter {#bloom-filter}

A **Bloom filter** is a [probabilistic data structure](https://ru.wikipedia.org/wiki/%D0%A4%D0%B8%D0%BB%D1%8C%D1%82%D1%80_%D0%91%D0%BB%D1%83%D0%BC%D0%B0) that allows you to quickly check whether an element belongs to a set. False positives are possible, but false negatives are not.

#### Local bloom index {#local-bloom-skip-index}

A local Bloom index is a special case of a [local index](#local-index): a probabilistic filter based on column values using a [Bloom filter](https://ru.wikipedia.org/wiki/%D0%A4%D0%B8%D0%BB%D1%8C%D1%82%D1%80_%D0%91%D0%BB%D1%83%D0%BC%D0%B0) that speeds up selective queries by skipping data fragments where the searched value is guaranteed to be absent. For more information, see [Bloom indexes](../dev/bloom-skip-indexes.md) and [local indexes](query_execution/local_indexes.md).

#### Column family {#column-family}

**Column family** or **column group** is a feature that allows storing subsets of columns of a [row table](#row-oriented-table) separately in a separate family or group. The main use case is storing some columns on other disk types (moving less important columns to HDD) or with different compression settings. If your workload requires many column families, consider using [column-oriented tables](#column-oriented-table).

#### Column encoding {#column-encoding}

**Column encoding** is a mechanism for optimizing data storage in table columns, which reduces the amount of disk space used and speeds up the execution of certain operations.

#### Time to Live {#ttl}

**Time to live** or **TTL** is a mechanism for automatically deleting old rows from a table asynchronously in the background. It is described in a separate article [{#T}](ttl.md).

### View {#view}

A **view** is a way to save a query and access its results as if they were a real table. The view itself does not store any data except the query text. The query stored in the view is executed each time you SELECT from it, generating the returned result. Any changes to the tables referenced by the view are immediately reflected in the results of reading from it.

{% if feature_view %}

Views can be user-defined or system.

#### User views {#user-view}

**User views** are created by the user using the [{#T}](../yql/reference/syntax/create-view.md) command. They are described in more detail in [{#T}](../concepts/datamodel/view.md).

{% endif %}

#### System views {#system-view}

**System views** are special views automatically created by the system for monitoring the state of a database and cluster. They are located in the special `.sys` directory in the root directory of each database. System views for databases are described in [{#T}](../dev/system-views.md); system views for a cluster, as well as access management issues, are described in [{#T}](../devops/observability/system-views.md).

### Topic {#topic}

A **message queue** is used for reliable asynchronous communication between different systems by passing messages. {{ ydb-short-name }} provides the infrastructure that ensures "exactly once" semantics in such communications. Using it, you can guarantee no lost messages or random duplicates.

A **topic** is a named entity in a message queue designed for interaction between [writers](#producer) and [readers](#consumer).

Several terms related to topics are given below. How {{ ydb-short-name }} topics work is explained in more detail in a separate article [{#T}](datamodel/topic.md).

#### Partition {#partition}

For horizontal scaling, topics are divided into individual elements called **partitions**. Thus, partitions are a unit of parallelism within a topic. Messages within each partition are ordered.

However, subsets of data managed by a single [data shard](#data-shard) or [column shard](#column-shard) may also be called partitions.

#### Offset {#offset}

An **offset** is a sequence number that identifies a message within a [partition](#partition).

#### Writer {#producer}

A **writer** or **producer** is an entity that writes new messages to a topic.

#### Reader {#consumer}

A **reader** or **consumer** is an entity that reads messages from a topic.

### Change data capture {#cdc}

**Change data capture** or **CDC** is a mechanism that allows you to subscribe to a **change stream** for a specific [table](#table). Technically, it is implemented on top of [topics](#topic). It is described in more detail in a separate article [{#T}](cdc.md).

#### Change stream {#changefeed}

A **change stream** is an ordered list of changes to a [table](#table) placed in a [topic](#topic).

### Backup collection {#backup-collection}

A **backup collection** is a [schema object](#scheme-object) that organizes full and incremental [backups](#backup) for selected [row tables](#row-oriented-table). Collections provide [point-in-time recovery](https://en.wikipedia.org/wiki/Point-in-time_recovery), support [backup chains](#backup-chain), and guarantee consistent recovery of multiple tables. A table can belong to only one backup collection at a time.

For more information, see [{#T}](datamodel/backup-collection.md).

#### Backup {#backup}

A **backup** is a copy of data at a specific point in time that can be used for data recovery. In the context of [backup collections](#backup-collection), there are two types:

- **Full backup**: A complete snapshot of all data in the collection. It serves as the basis for [backup chains](#backup-chain) and can be restored independently.
- **Incremental backup**: Captures only the changes (inserts, updates, deletes) since the previous backup. It requires the entire backup chain for restoration.

#### Backup chain {#backup-chain}

A **backup chain** is an ordered sequence of [backups](#backup) starting with a full backup, followed by zero or more incremental backups. Each incremental backup depends on all previous backups in the chain. Deleting any backup in the chain makes subsequent incremental backups unrecoverable.

{% if feature_async_replication == true %}

### Async replication instance {#async-replication-instance}

An **async replication instance** is a named entity that stores the settings of [async replication](async-replication.md) (connection settings, list of replicated objects, etc.). It can also be used to obtain information about the state of async replication: [initial scan progress](async-replication.md#initial-scan), [lag](async-replication.md#replication-of-changes), [errors](async-replication.md#error-handling), etc.

#### Replicated object {#replicated-object}

A **replicated object** is an object (for example, a table) for which async replication is configured.

#### Replica object {#replica-object}

A **replica object** is a "mirror copy" of the replicated object, automatically created by the async replication instance. It is typically read-only.

{% endif %}

{% if feature_transfer == true %}

### Transfer instance {#transfer-instance}

A **transfer instance** is a named entity that stores the settings of a [transfer](transfer.md), including connection settings and data transformation rules. It can also be used to obtain information about the transfer state, for example [errors](transfer.md#error-handling).

{% endif %}

### Coordination node {#coordination-node}

A **coordination node** is a schema object that allows client applications to create semaphores for coordinating their actions. Coordination nodes are used to implement distributed locks, service discovery, leader election, and other scenarios. Learn more about [coordination nodes](./datamodel/coordination-node.md).

#### Semaphore {#semaphore}

A **semaphore** is an object inside a [coordination node](#coordination-node) that provides a synchronization mechanism for distributed applications. Semaphores can be persistent or temporary and support operations such as creation, acquisition, release, and monitoring. Learn more about [semaphores in {{ ydb-short-name }}](./datamodel/coordination-node.md#semaphore).

{% if feature_resource_pool == true and feature_resource_pool_classifier == true %}

### Resource pool {#resource-pool}

A **resource pool** is a schema object that describes the limits imposed on resources (CPU, RAM, etc.) available for executing queries in this resource pool. A query is always executed in some resource pool. By `default`, all queries are executed in a resource pool named , which does not impose any restrictions. For more information about using resource pools, see [{#T}](../dev/resource-consumption-management.md).

### Resource pool classifier {#resource-pool-classifier}

A **resource pool classifier** is an object designed to manage the distribution of queries among [resource pools](#resource-pool). It describes the rules by which a resource pool is selected for each query. These classifiers are global for the entire [database](#database) and apply to all queries entering it. For more information about their usage, see [{#T}](../dev/resource-consumption-management.md).

{% endif %}

### YQL {#yql}

**YQL ({{ ydb-short-name }} Query Language)** is a high-level language for working with the system. It is a dialect of [ANSI SQL](https://en.wikipedia.org/wiki/SQL). There are many materials dedicated to YQL, including a [tutorial](../dev/yql-tutorial/index.md), [reference guide](../yql/reference/syntax/index.md), and [recipes](../yql/reference/recipes/index.md).

### Federated queries {#federated-queries}

**Federated queries** is a feature that allows you to execute queries against data stored in systems external to the {{ ydb-short-name }} cluster.

Below are explanations of several terms related to federated queries. How federated queries work in {{ ydb-short-name }} is explained in more detail in a separate article [{#T}](query_execution/federated_query/index.md).

#### External data source {#external-data-source}

An **external data source**, **external connection** — is metadata that describes how to connect to a supported external system to execute [federated queries](#federated-queries).

#### External table {#external-table}

An **external table** is metadata that describes a specific set of data that can be retrieved from an [external data source](#external-data-source).

#### Secret {#secret}

A **secret** is confidential metadata that requires special handling. For example, secrets can be used in definitions of [external data sources](#external-data-source) and represent entities such as passwords and tokens.

### Authentication token {#auth-token}

An **auth token** is a token used for [authentication](../security/authentication.md) in {{ ydb-short-name }}.

{{ ydb-short-name }} supports [different types of authentication](../security/authentication.md) and various token types.

### mTLS {#mtls}

**mTLS** (mutual TLS) is a [TLS](https://ru.wikipedia.org/wiki/Transport_Layer_Security) mode where not only does the client verify the server's certificate, but the server also requests and verifies the client's [client certificate](#client-certificate) when establishing a connection.

### Client certificate {#client-certificate}

A **client certificate** is a [digital certificate](https://en.wikipedia.org/wiki/X.509) issued and used by a client — an application, user, or [node {{ ydb-short-name }}](#node) — to confirm its identity when interacting with {{ ydb-short-name }}.

### Cluster schema {#scheme}

The **cluster schema {{ ydb-short-name }}** is the hierarchical namespace of the {{ ydb-short-name }} cluster. The top-level element of this namespace is the [cluster schema root](#scheme-root). The child elements of the cluster schema root are [databases](#database). Inside each database, you can create an arbitrary hierarchy of [objects](#scheme-object) (tables, topics, etc.) using nested directories.

### Database schema {#scheme-database}

A **database schema** is a subset of the cluster's hierarchical namespace that belongs to a database.

### Database root {#scheme-database-root}

The **database root** is the path to the database in the cluster schema.

### Schema root {#scheme-root}

The **cluster schema root** is the root element of the [namespace {{ ydb-short-name }}](datamodel/cluster-namespace.md), whose child elements are [databases](#database).

### Schema object {#scheme-object}

A database schema consists of **schema objects**, which can be databases, [tables](#table) (including [external tables](#external-table)), [topics](#topic), [directories](#folder), and so on.

For organizational convenience, schema objects form a hierarchy using [directories](#folder).

### Folder {#folder}

As in file systems, a **folder** or **directory** is a container for [schema objects](#scheme-object).

Directories can contain subdirectories, and such nesting can be of arbitrary depth.

### Access object {#access-object}

An **access object** during [authorization](../security/authorization.md) is an entity for which access rights and restrictions are configured. In {{ ydb-short-name }}, access objects are [schema objects](#scheme-object).

Each [schema object](#scheme-object) has an [owner](#access-owner) and an [access control list](#access-control-list) of rights granted to users and groups ([access subjects](#access-subject)).

### Access subject {#access-subject}

An **access subject** is an entity that can access [access objects](#access-object) and perform certain actions in the system.

Obtaining access during these requests and actions depends on the configured [access control lists](#access-control-list) and the subject's [access level](#access-level).

An access subject can be a [user](#access-user) or a [group](#access-group).

### Access right {#access-right}

An **[access right](../security/authorization.md#right)** is an entity that reflects permission for an [access subject](#access-subject) to perform a specific set of operations in a cluster or database on a specific [access object](#access-object).

### Inheritance of access rights {#access-right-inheritance}

**Inheritance of access rights** is a mechanism where [access rights](#access-right) granted on parent [access objects](#access-object) are inherited by child objects in the hierarchical database structure. This ensures that permissions granted at a higher level of the hierarchy apply to all lower levels unless they are [explicitly overridden](../reference/ydb-cli/commands/scheme-permissions.md#clear-inheritance).

### Access control list {#access-control-list}

An **[access control list](../security/authorization.md#right)** or **ACL** is a list of all [rights](#access-right) granted to [access subjects](#access-subject) (users and groups) on a specific [access object](#access-object).

### Access level {#access-level}

An **access level** provides an [access subject](#access-subject) with additional capabilities when working with [schema objects](#scheme-object), as well as the ability to perform operations on the cluster as a whole. {{ ydb-short-name }} uses hierarchical access levels:

- Database;
- Viewer;
- Monitoring;
- Administration.

The access level for a subject is configured using [access level lists](#access-level-list).

### Access level list {#access-level-list}

An **access level list** is a list of [SID](#access-sid)s of [access subjects](#access-subject) that are allowed a specific [access level](#access-level).

In {{ ydb-short-name }}, there are [several such lists](../reference/configuration/security_config.md#security-access-levels) that define who has which [access levels](#access-level).

For detailed information about access control lists, their hierarchy, and how they work, see the [Access Control Lists](../security/authorization.md#access-level-lists) section of the authorization documentation.

### Owner {#access-owner}

**[Owner](../security/authorization.md#owner)** — an [access subject](#access-subject) ([user](#access-user) or [group](#access-group)) that has full rights to a specific [access object](#access-object).

### User {#access-user}

**[User](../security/authorization.md#user)** — a person using {{ ydb-short-name }} to perform a specific function.

In {{ ydb-short-name }}, there are different types of users depending on how they are created:

- local users in {{ ydb-short-name }} databases;
- external users from third-party directories.

A user is identified by a [SID](#access-sid).

#### Local user {#local-user}

A user whose account is created directly in {{ ydb-short-name }} using the YQL command `CREATE USER` or during [initial security configuration](../security/builtin-security.md).

#### External user {#external-user}

A {{ ydb-short-name }} user whose account is created in a third-party directory, such as an LDAP directory or IAM system.

### Group {#access-group}

**[Group](../security/authorization.md#group)** or **access group** — a named set of [users](#access-user) and other groups with equal capabilities for their members.

A group is identified by a [SID](#access-sid).

### Role {#access-role}

A role is a named set of [access rights](#access-right) used to assign to [users](#access-user) or [groups](#access-group) of users.

Roles in {{ ydb-short-name }} are implemented using [groups](#access-group) that are created during the initial cluster deployment and assigned a specific [access list](#access-right) on the cluster schema root. For more information about roles, see the [{#T}](../security/builtin-security.md) article.

### SID {#access-sid}

**SID** or **security identifier** — a string of the form `<name>` or `<name>@<auth-domain>` that identifies an [access subject](../concepts/glossary.md#access-subject). It is used for [authentication](../security/authentication.md), [authorization](../security/authorization.md), in [access lists](#access-control-list), and in [access control lists](#access-level-list).

A SID identifies an individual [user](#access-user) or [group of users](#access-group).

The optional suffix `@<auth-domain>` identifies the source of the access subject, that is, the external directory or system from which it was obtained. For example, users or groups from an LDAP directory may have the suffix `@ldap`. The absence of a suffix means that the user or group was created and exists directly in {{ ydb-short-name }}.

### Query optimizer {#optimizer}

[**Query optimizer**](https://ru.wikipedia.org/wiki/%D0%9E%D0%BF%D1%82%D0%B8%D0%BC%D0%B8%D0%B7%D0%B0%D1%86%D0%B8%D1%8F_%D0%B7%D0%B0%D0%BF%D1%80%D0%BE%D1%81%D0%BE%D0%B2_%D0%A1%D0%A3%D0%91%D0%94) — a set of {{ ydb-short-name }} components responsible for transforming the logical representation of a query into a specific physically executable plan for obtaining the requested result. The main goal of the optimizer is to select, from all possible query execution plans, one that is sufficiently efficient in terms of predicted execution time and cluster resource consumption. It is described in more detail in a separate article [{#T}](query_execution/optimizer.md).

### Compilation cache {#compile-cache}

**Compilation cache** or **compile cache** — a cache of compiled queries on each [node](#node) of the cluster. It is used to avoid recompilation: if the query text is already in the node's cache, additional compilation is not performed. For more information, see the [Query Compilation Cache](../dev/system-views.md#compile-cache-queries) section.

## Advanced terminology {#advanced-terminology}

This section explains terms that are useful for [{{ ydb-short-name }} contributors](../contributor/index.md) and users who want to gain a deeper understanding of what happens inside the system.

### Actor implementation {#actor-implementation}

#### Actor system {#actor-system}

**Actor system** — a C++ library with an [implementation](https://github.com/ydb-platform/ydb/tree/main/ydb/library/actors) of the [actor model](https://en.wikipedia.org/wiki/Actor_model) for the needs of {{ ydb-short-name }}.

#### Actor service {#actor-service}

**Actor service** — an [actor](#actor) that has a known name and usually runs as a single instance on a [node](#node).

#### ActorId {#actorid}

**ActorId** — a unique identifier of an actor or [tablet](#tablet) in a [cluster](#cluster).

#### Actor system interconnect {#actor-system-interconnect}

**Actor system interconnect**, **interconnect** — the internal network layer of a [cluster](#cluster). All [actors](#actor) interact with each other in the system through the interconnect.

#### Local {#local}

**Local** — an [actor service](#actor-service) running on each [node](#node). It directly manages [tablets](#tablet) on its node and interacts with [Hive](#hive). It registers with Hive and receives commands to start tablets.

### ### Tablet implementation {#tablet-implementation}

A [**tablet**](#tablet) is an [actor](#actor) with persistent state. It includes a set of data that the tablet is responsible for and a state machine through which the tablet's data (or state) is modified. A tablet is a fault-tolerant entity because the tablet's data is stored in [distributed storage](#distributed-storage), which survives disk and node failures. The tablet automatically restarts on another [node](#node) in case of failure or overload of the previous one. Data in a tablet is modified sequentially, as the system infrastructure guarantees that there is no more than one [tablet leader](#tablet-leader) through which the tablet's data changes are performed.

A tablet solves the same problem as the [Paxos](https://ru.wikipedia.org/wiki/%D0%90%D0%BB%D0%B3%D0%BE%D1%80%D0%B8%D1%82%D0%BC_%D0%9F%D0%B0%D0%BA%D1%81%D0%BE%D1%81) and [Raft](https://ru.wikipedia.org/wiki/Raft_(%D0%B0%D0%BB%D0%B3%D0%BE%D1%80%D0%B8%D1%82%D0%BC)) algorithms in other systems, namely the problem of [distributed consensus](https://ru.wikipedia.org/wiki/%D0%9A%D0%BE%D0%BD%D1%81%D0%B5%D0%BD%D1%81%D1%83%D1%81_%D0%B2_%D1%80%D0%B0%D1%81%D0%BF%D1%80%D0%B5%D0%B4%D0%B5%D0%BB%D1%91%D0%BD%D0%BD%D1%8B%D1%85_%D0%B2%D1%8B%D1%87%D0%B8%D1%81%D0%BB%D0%B5%D0%BD%D0%B8%D1%8F%D1%85). From a technical standpoint, a tablet implementation can be described as a Replicated State Machine (RSM) on top of a shared log, since the tablet's state is fully described by an ordered log of commands stored in a distributed and fault-tolerant storage.

During execution, the tablet's state machine is managed by three components:

1. The common tablet part ensures log consistency and recovery in case of failures.
2. The **executor** is an abstraction of a local database, namely the data structures and code that organize the work with data stored by the tablet.
3. An actor with user code that implements the specific logic of a particular tablet type.

In {{ ydb-short-name }}, there are several types of specialized tablets that store various data for different tasks. Many {{ ydb-short-name }} features, such as [tables](#table) and [topics](#topic), are implemented as different types of tablets. Thus, reusing the tablet infrastructure is one of the key means of extensibility of {{ ydb-short-name }} as a platform.

Typically, in a {{ ydb-short-name }} cluster, orders of magnitude more tablets are running compared to the processes or threads that other systems would use for a cluster of a similar size. In a {{ ydb-short-name }} cluster, hundreds of thousands or millions of tablets can easily run simultaneously.

Since a tablet stores its state in [distributed storage](#distributed-storage), it can be (re)started on any node in the cluster. Tablets are identified using a [TabletID](#tabletid), a 64-bit number assigned when the tablet is created.

### ### Tablet leader {#tablet-leader}

A **tablet leader** is the current active leader of a given tablet. The tablet leader accepts commands, assigns them an order, and confirms them to the outside world. It is guaranteed that at any given time, there is no more than one leader for each tablet.

### ### Tablet candidate {#tablet-candidate}

A **tablet candidate** is one of the election participants that wants to become the [leader](#tablet-leader) of a given tablet. If a candidate wins the election, it becomes the tablet leader.

### ### Tablet replica {#tablet-follower}

A **tablet replica**, also known as a **tablet follower** or **hot standby**, is a copy of the [tablet leader](#tablet-leader) that applies the log of commands accepted by the leader (with some delay). A tablet can have zero or more replicas. Replicas perform two main functions:

* In case the leader terminates or fails, replicas are the preferred [candidates](#tablet-candidate) for the new leader role, as they can become the leader much faster than other candidates because they have applied most of the log.
* Replicas can respond to read‑only requests if the client explicitly selects an optional relaxed transaction mode that allows stale reads.

### ### Tablet generation {#tablet-generation}

A **tablet generation** is a number that identifies the reincarnation of the tablet leader. It changes only when a new leader is elected and always increases.

### ### Tablet local database {#local-database}

**Tablet local database** or **local database** is a set of data structures and associated code that manage the state of a tablet and the data it stores. Logically, the state of a local database is represented by a set of tables, very similar to relational tables. Modifications to the state of a local database are made by local tablet transactions created by the tablet's user actor.

Each table in the local database is stored as an [LSM-tree](#lsm-tree).

#### Log-structured merge-tree {#lsm-tree}

A **[Log-structured merge-tree](https://ru.wikipedia.org/wiki/LSM-%D0%B4%D0%B5%D1%80%D0%B5%D0%B2%D0%BE)** or **LSM-tree** is a data structure designed to optimize write and read performance in storage systems. It is used in {{ ydb-short-name }} to store [local database](#local-database) tables and [VDisks](#vdisk) data.

#### MemTable {#memtable}

All data written to [local database](#local-database) tables is initially stored in an in-memory data structure called a **MemTable**. When the MemTable reaches a specified size, it is flushed to disk as an immutable data structure called an [SST](#sst).

#### Sorted string table {#sst}

A **Sorted string table** or **SST** is an immutable data structure that stores table rows sorted by key, facilitating efficient key lookups and range scans. Each SST consists of a continuous series of small data pages, typically about 7 KiB each, which further optimizes reading data from disk. An SST is usually part of an [LSM-tree](#lsm-tree).

#### Tablet pipe {#tablet-pipe}

A **tablet pipe** or **TabletPipe** is a virtual connection that can be established with a tablet. It includes locating the [tablet leader](#tablet-leader) by [TabletID](#tabletid). This is the recommended way to interact with a tablet. The term **opening a pipe to a tablet** describes the process of resolving (locating) a tablet in the cluster and establishing a virtual communication channel with it.

#### TabletID {#tabletid}

**TabletID** is a unique identifier for a [tablet](#tablet) within a cluster.

#### Bootstrapper {#bootstrapper}

**Bootstrapper** is the primary mechanism for starting tablets, used for system tablets (e.g., [Hive](#hive), [DS controller](#ds-controller), root [SchemeShard](#scheme-shard)). [Hive](#hive) initializes the remaining tablets.

### Shared cache {#shared-cache}

**Shared cache** is an [actor](#actor) that stores data pages recently read from [distributed storage](#distributed-storage). Caching these pages reduces disk I/O operations and speeds up data retrieval, improving overall system performance.

### Memory controller {#memory-controller}

**Memory controller** is an [actor](#actor) that manages [memory limits](../reference/configuration/memory_controller_config.md) in {{ ydb-short-name }}.

### Spilling {#spilling}

**Spilling** is a memory management mechanism in {{ ydb-short-name }} that temporarily offloads intermediate query data to external storage when such data exceeds the available RAM of a node. In {{ ydb-short-name }}, disk is currently used for spilling.

For more details on spilling, see [{#T}](query_execution/spilling.md).

### Tablet types {#tablet-types}

[Tablets](#tablet) can be considered a framework for building reliable components operating in a distributed system. Many components of {{ ydb-short-name }} — both system and user-data components — are implemented using this framework; the main ones are listed below.

#### SchemeShard {#scheme-shard}

**SchemeShard** or **Scheme shard** is a system tablet that stores the database schema, including metadata for user [tables](#table), [topics](#topic), etc.

Additionally, there is a **root SchemeShard** that stores information about databases created in the cluster.

#### DataShard {#data-shard}

**DataShard** or **Data shard** is a tablet that manages a segment of a [row-based user table](datamodel/table.md#row-oriented-tables). A logical user table is divided into segments based on continuous ranges of the table's primary key. Each such range is managed by a separate DataShard tablet. The range itself is also called a [partition](#partition). The DataShard tablet stores data row by row, which is efficient for OLTP workloads.

#### ColumnShard {#column-shard}

**ColumnShard** or **Column shard** is a tablet that stores a data segment of a [column-based user table](datamodel/table.md#column-oriented-tables).

#### KeyValue Tablet {#kv-tablet}

**KeyValue**, **KV Tablet**, or **key-value tablet** is a tablet that implements a simple key → value mapping, where keys and values are strings. It also has several specific features, such as locks.

#### PersQueue Tablet {#pq-tablet}

**PersQueue** or **persistent queue tablet** is a tablet that implements the [topic](#topic) concept. Each topic consists of one or more partitions, and each partition is managed by a separate PQ tablet instance.

#### TxAllocator {#txallocator}

**TxAllocator** or **transaction allocator** is a system tablet that allocates unique transaction identifiers ( [TxID](#txid)) in the cluster. Typically, a cluster has several such tablets, from which the [transaction proxy](#transaction-proxy) pre-allocates and caches ranges for local issuance within a single process.

#### Coordinator {#coordinator}

**Coordinator** is a system tablet that ensures the global ordering of transactions. The coordinator's task is to assign a logical time [PlanStep](#planstep) to each transaction planned through this coordinator. Each transaction is assigned exactly one coordinator, selected by hashing its [TxId](#txid).

#### Mediator {#mediator}

**Mediator** is a system tablet that distributes transactions planned by [coordinators](#coordinator) among transaction participants. Mediators ensure the advancement of global time. Each transaction participant is associated with exactly one mediator. Mediators eliminate the need for a full set of connections between all coordinators and all participants of all transactions.

#### Hive {#hive}

**Hive** is a system tablet responsible for launching and managing other tablets. Its responsibilities include moving tablets between nodes in case of a [node](#node) failure or overload.{% if audience != "corp" %} You can learn more about Hive in a [separate article](../contributor/hive.md).{% endif %}

#### CMS {#cms}

**CMS** or **cluster management system** is a system tablet responsible for managing information about the current state of the [cluster {{ ydb-short-name }}](#cluster). This information is used for performing gradual cluster restarts without impacting user workloads, maintenance, cluster reconfiguration, and so on.

#### NodeBroker {#node-broker}

**NodeBroker** is a system tablet responsible for registering [dynamic nodes](#dynamic) in the cluster.

#### BSController {#ds-controller}

**BSController**, **blob storage controller**, or **BS controller** manages the dynamic configuration of the distributed storage, including information about [PDisk](#pdisk), [VDisk](#vdisk), and [storage groups](#storage-group). It interacts with the [node warden](#node-warden) to start various distributed storage components. It interacts with [Hive](#hive) to allocate [channels](#channel) to tablets.

#### Console {#console}

**Console** is a system tablet responsible for storing the [dynamic configuration](../devops/configuration-management/configuration-v1/dynamic-config.md) and delivering it to cluster nodes.

#### Kesus {#kesus}

**Kesus** is a tablet that implements a [coordination node](datamodel/coordination-node.md).

#### SysViewProcessor {#sys-view-processor}

**SysViewProcessor** is a tablet that stores data for some of the [system views](../dev/system-views.md).

{% if feature_serial %}

#### SequenceShard {#sequence-shard}

**SequenceShard** is a tablet that serves Sequence objects, which are used to implement [serial data types](../yql/reference/types/serial.md).

{% endif %}

{% if feature_async_replication %}

#### ReplicationController {#replication-controller}

**ReplicationController** is a tablet responsible for the [asynchronous replication](async-replication.md) process.

{% endif %}

#### StatisticsAggregator {#statistics-aggregator}

**StatisticsAggregator** is a tablet responsible for collecting statistics used in cost optimization.

### Slot {#slot}

A **Slot** in {{ ydb-short-name }} can be used in two contexts:

* **Slot** — a portion of server resources allocated to run one {{ ydb-short-name }} [node](#node). The typical slot size is 10 CPU cores and 50 GB of RAM. Slots are used if the {{ ydb-short-name }} cluster is deployed on servers or virtual machines with sufficient resources to host multiple slots.
* **VDisk slot** or **VSlot** — a share of a [PDisk](#pdisk) that can be allocated to one of the [VDisk](#vdisk) instances.

### State storage {#state-storage}

**State storage**, **state storage** or **StateStorage** is a distributed service that stores information about tablets, namely:

* The current tablet leader or its absence.
* Tablet replicas.
* The generation and step of the tablet `(generation:step)`.

State storage is used as a service for resolving tablet names, i.e., obtaining an [ActorId](#actorid) by [TabletID](#tabletid). StateStorage is also used in the process of [tablet leader election](#tablet-leader).

The information in state storage is volatile. Thus, it is lost when power is disconnected or the process is restarted. Despite its name, this service is not a permanent long-term storage. It only contains information that is easy to recover and that should not be durable. However, state storage stores information on multiple nodes to minimize the impact of node failures. This service can also be used to gather a quorum, which is used for tablet leader election.

Due to its nature, the state storage service operates on a best-effort basis. For example, the absence of multiple tablet leaders is guaranteed through the leader election protocol on [distributed storage](#distributed-storage), not on state storage.

For more details on the StateStorage architecture and related subsystems, see the Metadata distribution services section.

### Board {#board}

**Board** is a distributed service designed to store metadata as key-value pairs. It is used, among other things, to store information about [endpoints](../concepts/connect.md#endpoint).

For more details on the Board architecture and related subsystems, see the Metadata distribution services section.

### SchemeBoard {#scheme-board}

**SchemeBoard** is a distributed service designed to store metadata as key-value pairs. It is used, among other things, to store information about [schemas](#global-schema).

For more details on the SchemeBoard architecture and related subsystems, see the Metadata distribution services section.

#### Compaction {#compaction}

**Compaction** or **compaction** is an internal background process of rebuilding the [LSM-tree](#lsm-tree) data. Data in [VDisk](#vdisk) and [local databases](#local-database) is organized as LSM-trees. Therefore, a distinction is made between **VDisk compaction** and **tablet compaction**. The compaction process is usually quite resource-intensive, so measures are taken to minimize the associated overhead, for example, by limiting the number of simultaneously running compactions.

#### gRPC proxy {#grpc-proxy}

**gRPC proxy** is a proxy system for external user requests. Client requests enter the system via the [gRPC](https://grpc.io) protocol, then the proxy component translates them into internal calls to execute these requests, transmitted via [interconnect](#actor-system-interconnect). This proxy provides an interface for both request-response and bidirectional streaming.

### Distributed configuration {#distributed-configuration}

**Distributed configuration** or **DistConf** is an internal cluster [configuration](../devops/configuration-management/configuration-v2/config-overview.md) mechanism that provides startup and configuration of [static nodes](#static-node), automatic management of the [static storage group](#static-group) and [State Storage](../concepts/glossary.md#state-storage). Distributed configuration starts before any [tablets](#tablet), [storage groups](#storage-group), and [State Storage](../concepts/glossary.md#state-storage).

For more details on the distributed configuration architecture, see [{#T}](../contributor/configuration-v2.md).

### Distributed storage implementation {#distributed-storage-implementation}

**Distributed Storage** is a distributed fault-tolerant data storage layer that stores binary records called [LogoBlobs](#logoblob), addressed using a specific type of identifier called [LogoBlobID](#logoblobid). Thus, Distributed Storage is a key-value store that maps a LogoBlobID to a string of up to 10 MB. Distributed Storage consists of multiple [storage groups](#storage-group), each of which is an independent data repository.

Distributed Storage stores immutable data, with each immutable data block identified by a specific LogoBlobID key. The Distributed Storage API is very specific, intended only for use by [tablets](#tablet) to store their data and change logs. Thus, it is not intended for general-purpose data storage. Data in Distributed Storage is deleted using special barrier commands. Due to the absence of mutations in its interface, Distributed Storage can be implemented without implementing [distributed consensus](https://ru.wikipedia.org/wiki/%D0%9A%D0%BE%D0%BD%D1%81%D0%B5%D0%BD%D1%81%D1%83%D1%81_%D0%B2_%D1%80%D0%B0%D1%81%D0%BF%D1%80%D0%B5%D0%B4%D0%B5%D0%BB%D1%91%D0%BD%D0%BD%D1%8B%D1%85_%D0%B2%D1%8B%D1%87%D0%B8%D1%81%D0%BB%D0%B5%D0%BD%D0%B8%D1%8F%D1%85). Distributed Storage is only one of the components that tablets use to implement distributed consensus.

#### LogoBlob {#logoblob}

**LogoBlob** is a set of binary immutable data identified by a [LogoBlobID](#logoblobid) and stored in [Distributed Storage](#distributed-storage). The data block size is limited at the [VDisk](#vdisk) level and above in the stack. Currently, the maximum data block size that VDisks can handle is 10 MB.

#### LogoBlobID {#logoblobid}

**LogoBlobID** is the identifier of a [LogoBlob](#logoblob) in [Distributed Storage](#distributed-storage). It has a structure of the form `[TabletID, Generation, Step, Channel, Cookie, BlobSize, PartID]`. The main elements of LogoBlobID are:

* `TabletID` is the [ID](#tabletid) of the tablet that owns the LogoBlob.
* `Generation` is the generation of the tablet in which the data block was written.
* `Channel` is the [channel](#channel) of the tablet on which the LogoBlob is written.
* `Step` is an incremental counter, usually within the tablet generation.
* `Cookie` is a unique identifier of the data block within a single `Step`. The cookie is typically used when writing multiple data blocks to one `Step`.
* `BlobSize` is the size of the LogoBlob.
* `PartID` is the identifier of the data block part. It is important when the original LogoBlob is split into parts using [erasure coding](#erasure-coding), and the parts are written to the corresponding [VDisk](#vdisk) and [storage groups](#storage-group).

#### Replication {#replication}

**Replication** is a process that ensures a sufficient number of copies (replicas) of data are available to maintain the desired availability characteristics of a {{ ydb-short-name }} cluster. It is typically used in geo-distributed {{ ydb-short-name }} clusters.

#### Erasure coding {#erasure-coding}

[**Erasure coding**](https://ru.wikipedia.org/wiki/%D0%A1%D1%82%D0%B8%D1%80%D0%B0%D1%8E%D1%89%D0%B8%D0%B9_%D0%BA%D0%BE%D0%B4) is a data encoding method where the original data is supplemented with redundancy and split into multiple fragments, enabling the recovery of the original data if one or more fragments are lost. It is widely used in {{ ydb-short-name }} clusters with a single [availability zone](#regions-az), unlike [replication](#replication) with 3 replicas. For example, the most popular erasure coding scheme 4+2 provides the same reliability as three replicas, with a space overhead of 1.5 versus 3.

#### PDisk {#pdisk}

**PDisk** (also known as **physical disk**) is a component that controls a physical disk drive (block device). In other words, PDisk is a subsystem that implements an abstraction similar to a specialized file system on top of block devices (or files emulating a block device for testing purposes). PDisk provides data integrity control (including [erasure coding](#erasure-coding) of sector groups to recover data on individual damaged sectors, integrity control using checksums), transparent encryption of all data on the disk, and transactional guarantees for disk operations (write confirmation strictly after `fsync`).

PDisk contains a scheduler that ensures sharing of the device's bandwidth among multiple clients ( [VDisk](#vdisk)). PDisk divides the block device into blocks called [slots](#slot) (about 128 megabytes in size; smaller blocks are also allowed). At any given time, no more than one VDisk can own each slot. PDisk also maintains a recovery log shared by the PDisk service records and all VDisks.

#### VDisk {#vdisk}

**VDisk** or **virtual disk** is a component that implements data storage of the [distributed storage](#distributed-storage) [LogoBlob](#logoblob) on a [PDisk](#pdisk). A VDisk stores all its data on a PDisk. One VDisk corresponds to one PDisk, but typically several VDisks are associated with one PDisk. Unlike PDisk, which hides blocks and logs behind it, VDisk provides an interface at the LogoBlob and [LogoBlobID](#logoblobid) level, such as writing a LogoBlob, reading LogoBlobID data, and deleting a set of LogoBlobs using a special command. A VDisk is a member of a [storage group](#storage-group). The VDisk itself is local, but many VDisks in a given group provide reliable data storage. VDisks in a group synchronize data with each other and replicate data in case of loss. The set of VDisks in a storage group forms a distributed RAID.

#### Yard {#yard}

**Yard** is the name of the [PDisk](#pdisk) API. It allows a [VDisk](#vdisk) to read and write data to blocks and logs, reserve blocks, delete blocks, and transactionally acquire and return ownership of blocks. In some contexts, Yard can be considered synonymous with PDisk.

#### Skeleton {#skeleton}

**Skeleton** is an [actor](#actor) that provides an interface to a [VDisk](#vdisk).

#### SkeletonFront {#skeletonfront}

**SkeletonFront** is a proxy actor for Skeleton that controls the flow of messages coming into Skeleton.

#### Proxy {#ds-proxy}

**Distributed storage proxy**, **DS proxy**, **BS proxy**, **DS-proxy**, or **BS-proxy** acts as a client library for performing operations with [distributed storage](#distributed-storage). The users of the DS proxy are [tablets](#tablet) that write to and read from distributed storage. The DS proxy hides the distributed nature of distributed storage from the user. The task of the DS proxy is to write to a quorum of [VDisks](#vdisk), perform retries when necessary, and control the write/read flow to prevent VDisk overload.

Technically, the DS proxy is implemented as an [actor service](#actor-service) launched by the [node warden](#node-warden) on each node for each storage group, handling all requests to the group (writing, reading, and deleting [LogoBlobs](#logoblob), locking the group). When writing data, the DS proxy performs [erasure coding](#erasure-coding) of the data, splitting the LogoBlob into parts that are then sent to the corresponding VDisks. The DS proxy performs the reverse process when reading, receiving parts from VDisks and reconstructing the LogoBlob from them.

#### Node warden {#node-warden}

**Node warden** or `BS_NODE` is an [actor service](#actor-service) on each cluster node that launches [PDisks](#pdisk), [VDisks](#vdisk), and [DS proxies](#ds-proxy) of [static storage groups](#static-group) when the node starts. It also interacts with the [DS controller](#ds-controller) to launch PDisk, VDisk, and DS proxies of [dynamic groups](#dynamic-group). The DS proxy of dynamic groups is launched on demand: node warden processes "undelivered" messages to the DS proxy, launches the corresponding DS proxies, and receives group configuration from the DS controller.

#### Fail realm {#fail-realm}

A **fail realm** is a set of [failure domains](#fail-domain) that can fail simultaneously due to a common cause. A correlated failure of two [VDisks](#vdisk) in the same fail realm is more likely than a failure of two VDisks from different fail realms.

An example of a fail realm is a set of equipment located in one [data center, or availability zone](#regions-az), which can fail entirely due to a natural disaster, a large-scale power outage, or another similar event.

#### Failure domain {#fail-domain}

A **fail domain** is a set of equipment that can fail simultaneously. A correlated failure of two [VDisk](#vdisk) within the same fail domain is more likely than a failure of two VDisks from different fail domains. In the case of different fail domains, the probability of simultaneous failure also depends on whether the domains in question belong to the same fail realm or different ones.

An example of a fail domain is a set of disks connected to a single server, since all disks of a particular server may become unavailable if the server's power supply or network controller fails. Typically, all servers located in one [server rack](#rack) are considered part of a common fail domain, since power or network connectivity issues at the rack level make all equipment in it unavailable. Thus, a typical fail domain corresponds to a server rack (if the [cluster](#cluster) is configured with the equipment's rack topology in mind) or to an individual server.

Failures at the fail domain level are automatically handled by {{ ydb-short-name }} without stopping the cluster.

#### Distributed storage channel {#channel}

A **distributed storage channel**, **DS channel**, or **channel** is a logical connection between a [tablet](#tablet) and a [distributed storage](#distributed-storage) group. A tablet can write data to different channels, and each channel maps to a specific [storage group](#storage-group). Having multiple channels allows a tablet to:

* Write more data than a single storage group can contain.
* Store different [LogoBlob](#logoblob) in different storage groups, with different properties, such as erasure coding or on different media (HDD, SSD, NVMe).

### Distributed transaction implementation {#transaction-implementation}

The following explains terms related to the implementation of [distributed transactions](#transactions).{% if oss == true %} The implementation itself is described in a separate article [{#T}](../contributor/datashard-distributed-txs.md).{% endif %}

#### Deterministic transactions {#deterministic-transactions}

Distributed transactions in {{ ydb-short-name }} are inspired by the research paper [Building Deterministic Transaction Processing Systems without Deterministic Thread Scheduling](http://cs-www.cs.yale.edu/homes/dna/papers/transactions-wodet11.pdf) by Alexander Thomson and Daniel J. Abadi from Yale University. The paper introduced the concept of **deterministic transaction processing**, which allows efficient processing of distributed transactions. The original paper imposed restrictions on the types of operations that could be performed this way. Since these restrictions interfered with real user scenarios, {{ ydb-short-name }} evolved its algorithms to execute them, using deterministic transactions as stages for executing user transactions with additional orchestration and locking.

#### Optimistic locking {#optimistic-locking}

As in many other database management systems, queries in {{ ydb-short-name }} can place locks on specific data fragments, such as table rows, to ensure that concurrent changes do not lead to an inconsistent state. However, {{ ydb-short-name }} checks these locks not at the start of transactions, but when attempting to commit them. The first approach is called **pessimistic locking** (used, for example, in PostgreSQL), and the second is called **optimistic locking** (used in {{ ydb-short-name }}).

#### Transaction lock invalidation {#tli}

**Transaction Lock Invalidation** (**TLI**) is a standard behavior of {{ ydb-short-name }} when parallel transactions conflict within [optimistic locks](#optimistic-locking). If one transaction (the violator) writes data and thereby breaks the locks of another transaction (the victim), {{ ydb-short-name }} detects this when the victim commits and rolls it back with the `transaction locks invalidated` error. For more details on TLI diagnostics, see [{#T}](../troubleshooting/performance/queries/transaction-lock-invalidation.md).

#### Prepare phase {#prepare-stage}

The **prepare phase** is a transaction phase during which the transaction body is registered on all participating shards.

#### Execution phase {#execute-stage}

The **execution phase** is a transaction phase during which the scheduled transaction is executed and a response is generated.

In some cases, instead of [preparation](#prepare-stage) and execution, the transaction is executed immediately and a response is generated. For example, this happens for transactions that affect only one shard or for consistent reads from a data snapshot.

#### Dirty operations {#dirty-operations}

For read-only transactions, similar to "read uncommitted" in other database management systems, reading data that has not yet been committed to disk may be required. This is called **dirty operations**.

#### Read-write set {#rw-set}

A **read-write set**, **RW-set**, is a set of data that will participate in the execution of a [distributed transaction](#transactions). It combines the read set data that will be read and the write set for which modifications will be made.

#### Read set {#read-set}

A **read set**, **ReadSet data**, is what the participating shards send during transaction execution. For data transactions, it may contain information about the state of [optimistic locks](#optimistic-locking), the shard's readiness to commit, or a decision to cancel the transaction.

#### Transaction proxies {#transaction-proxy}

A **transaction proxy** or `TX_PROXY` is a service that orchestrates the execution of many [distributed transactions](#transactions): sequential phases, phase execution, scheduling, and result aggregation. In the case of direct orchestration by other actors (for example, QP data transactions), it is used for caching and allocating unique [TxIDs](#txid).

#### Transaction flags {#txflags}

**Transaction flags** or **TxFlags** is a bitmask of flags that modify transaction execution in some way.

#### Transaction ID {#txid}

A **transaction ID** or **TxID** is a unique identifier assigned to each transaction when it is accepted by {{ ydb-short-name }}.

#### Transaction order ID {#transaction-order-id}

A **transaction order ID** is a unique identifier assigned to each transaction during scheduling. It consists of a [PlanStep](#planstep) and a [Transaction ID](#txid).

#### Plan step {#planstep}

A **plan step**, **step**, or **PlanStep** is the logical time at which a set of transactions is scheduled to execute.

#### Mediator time {#mediator-time}

During the execution of distributed transactions, **mediator time** is the logical time up to which (inclusive) the participating shard must know the entire execution plan. It is used to advance time when there are no transactions on a particular shard, to determine whether it can read from a snapshot.

#### MiniKQL {#minikql}

**MiniKQL** is a language that allows expressing a single [deterministic transaction](#deterministic-transactions) in the system. It is a functional, strongly typed language. Conceptually, the language describes a graph of reading from the database, performing computations on the read data, and writing results to the database and/or to a special document representing the query result (for display to the user). A MiniKQL transaction must explicitly specify its read set (the data to be read) and assume deterministic selection of execution branches (for example, no randomness).

MiniKQL is a low-level language. End users of the system only see queries in [YQL](#yql), which relies on MiniKQL in its implementation.

#### Query Processor {#kqp}

**Query Processor** or **QP** (formerly **KQP**) is a {{ ydb-short-name }} component responsible for orchestrating the execution of user queries and generating the final response.

### Global schema {#global-schema}

**Global schema**, **global scheme**, or **database schema** is the schema of all data stored in a [database](#database). It consists of [tables](#table) and other entities such as [topics](#topic). The metadata about these entities is called the global schema. The term is used in contrast to **local schema**, which refers to the data schema inside a [tablet](#tablet). {{ ydb-short-name }} users never see the local schema and work only with the global schema.

### KiKiMR {#kikimr}

**KiKiMR** is the former name of {{ ydb-short-name }}, used before it became an [open source product](https://github.com/ydb-platform/ydb). It may still be encountered in source code, old articles and videos, etc.
