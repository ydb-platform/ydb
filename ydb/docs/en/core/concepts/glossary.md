# {{ ydb-short-name }} glossary

<<<<<<< HEAD
This article is an overview of terms and definitions used in {{ ydb-short-name }} and its documentation. It [starts with key terms](#key-terminology) that will be useful to get acquainted with early when you start working with {{ ydb-short-name }}, while the rest of it is more [advanced](#advanced-terminology) and might be helpful later on.

## Key terminology {#key-terminology}

This section explains terms that are useful to any person working with {{ ydb-short-name }} regardless of their role and use case.
=======
This article provides an overview of the terms and definitions used in {{ ydb-short-name }} and its documentation. It [begins with key terms](#key-terminology) that are useful to get familiar with early in your work with {{ ydb-short-name }}, and the rest of the article contains [more advanced terms](#advanced-terminology) that may be useful later.

## Key terminology {#key-terminology}

This section describes terms that are useful to anyone working with {{ ydb-short-name }}, regardless of their role or use case.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

### Cluster {#cluster}

A {{ ydb-short-name }} **cluster** is a set of interconnected {{ ydb-short-name }} [nodes](#node) that communicate with each other to serve user queries and reliably store user data. These nodes form one of the supported [cluster topologies](#topology), which directly affects the cluster's reliability and performance characteristics.

{{ ydb-short-name }} clusters are multitenant and can contain multiple isolated [databases](#database).

### Database {#database}

<<<<<<< HEAD
Like in most database management systems, a **database** in {{ ydb-short-name }} is a logical container for other entities like [tables](#table). However, in {{ ydb-short-name }}, the namespace inside databases is hierarchical like in [virtual file systems](https://en.wikipedia.org/wiki/Virtual_file_system), and thus [folders](#folder) allow for further organization of entities.

Another essential characteristic of {{ ydb-short-name }} databases is that they typically have dedicated compute resources allocated to them. Hence, creating a database requires additional operations from [DevOps engineers](../devops/index.md).
=======
As in most database management systems, a **database** in {{ ydb-short-name }} is a logical container for other entities, such as [tables](#table). However, in {{ ydb-short-name }}, the namespace within databases is hierarchical, like in [virtual file systems](https://en.wikipedia.org/wiki/Virtual_file_system), and thus [directories](#folder) allow for a more structured organization of entities.

Another important characteristic of databases {{ ydb-short-name }} is that they are usually allocated dedicated computing resources. As a result, creating a database requires additional actions by [DevOps engineers](../devops/index.md).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

### Node {#node}

A {{ ydb-short-name }} **node** is a server process running an executable called `ydbd`. A physical server or virtual machine can run multiple {{ ydb-short-name }} nodes, which is common. Thus, in the context of {{ ydb-short-name }}, nodes are **not** synonymous with hosts.

Given {{ ydb-short-name }} follows the approach of separated storage and compute layers, `ydbd` has multiple operation modes that determine the node type. The available node types are explained below.

#### Database node {#database-node}

<<<<<<< HEAD
**Database nodes** (also known as **tenant nodes** or **compute nodes**) serve user queries addressed to a specific logical [database](#database). Their state is only in memory and can be recovered from the [Distributed Storage](#distributed-storage). All database nodes of a given [{{ ydb-short-name }} cluster](topology.md) can be considered its compute layer. Thus, adding database nodes and allocating extra CPU and RAM to them are the main ways to increase the database's compute resources.
=======
**Database nodes** (also known as **tenant nodes** or **compute nodes**) process user queries addressed to a specific logical [database](#database). Their state is only in RAM and can be restored from [distributed storage](#distributed-storage). The set of database nodes of a given [cluster {{ ydb-short-name }}](topology.md) can be considered the compute layer of that cluster. Thus, adding database nodes and allocating additional resources (CPU and RAM) to them are the main ways to increase the compute resources of a database.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

The main role of database nodes is to run various [tablets](#tablet) and [actors](#actor), as well as accept incoming requests via various endpoints.

#### Storage node {#storage-node}

<<<<<<< HEAD
**Storage nodes** are stateful and responsible for long-term persisting pieces of data. All storage nodes of a given [{{ ydb-short-name }} cluster](#cluster) are called [Distributed Storage](#distributed-storage) and can be considered the cluster's storage layer. Thus, adding extra storage nodes and their disks are the main ways to increase the cluster's storage capacity and input/output throughput.
=======
**Storage nodes** are stateful nodes responsible for long-term storage of data fragments. The set of storage nodes of a given [cluster {{ ydb-short-name }}](#cluster) is called [distributed storage](#distributed-storage) and can be considered as the storage layer of that cluster. Thus, adding additional storage nodes and their disks is the primary way to increase the storage capacity and I/O throughput of the cluster.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### Hybrid node {#hybrid-mode}

A **hybrid node** is a process that simultaneously serves both roles of a [database](#database-node) and [storage](#storage-node) node. Hybrid nodes are often used for development purposes. For instance, you can run a container with a full-featured {{ ydb-short-name }} containing only one process, `ydbd`, in hybrid mode. They are rarely used in production environments.

#### Static node {#static-node}

<<<<<<< HEAD
**Static nodes** are manually configured during the initial cluster initialization or re-configuration. Typically, they play the role of [storage nodes](#storage-node), but technically, it is possible to configure them to be [database nodes](#database-node) as well.
=======
**Static nodes** are configured manually during initial cluster initialization or reconfiguration. Typically, they serve as [storage nodes](#storage-node), but it is technically possible to configure them as [database nodes](#database-node) as well.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### Dynamic node {#dynamic-node}

<<<<<<< HEAD
**Dynamic nodes** are added and removed from the cluster on the fly. They can only play the role of [database nodes](#database-node).
=======
**Dynamic nodes** are added to and removed from the cluster on the fly. They can only serve as [database nodes](#database-node).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

### Distributed storage {#distributed-storage}

**Distributed storage**, **Blob storage**, or **BlobStorage** is a distributed fault-tolerant data persistence layer of {{ ydb-short-name }}. It has a specialized API designed for storing immutable pieces of [tablet's](#tablet) data.

Multiple terms related to the [distributed storage implementation](#distributed-storage-implementation) are covered below.

### Storage group {#storage-group}

<<<<<<< HEAD
A **storage group**, **Distributed storage group**, or **Blob storage group** is a location for reliable data storage similar to [RAID](https://en.wikipedia.org/wiki/RAID), but using disks of multiple servers. Depending on the chosen [cluster topology](#topology), storage groups use different algorithms to ensure high availability, similar to [standard RAID levels](https://en.wikipedia.org/wiki/Standard_RAID_levels).
=======
A **storage group** is a place for reliable data storage, similar to [RAID](https://en.wikipedia.org/wiki/RAID) but using disks from multiple servers. Depending on the chosen [cluster topology](#topology), storage groups use different algorithms to ensure high availability, similar to [standard RAID levels](https://en.wikipedia.org/wiki/Standard_RAID_levels).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

[Distributed storage](#distributed-storage) typically manages a large number of relatively small storage groups. Each group can be assigned to a specific [database](#database) to increase disk capacity and input/output throughput available to this database.

<<<<<<< HEAD
[Static](#static-group) and [dynamic](#dynamic-group) storage groups are physical, meaning their data is stored directly on [VDisks](#vdisk).

#### Static group {#static-group}

A **static group** is a special [storage group](#storage-group) created during the initial cluster deployment. Its primary role is to store system [tablet's](#tablet) data, which can be considered cluster-wide metadata.
=======
[Static](#static-group) and [dynamic](#dynamic-group) storage groups are physical, meaning their data is placed directly on [VDisk](#vdisk)s.

#### Static group {#static-group}

A **static group** is a special [storage group](#storage-group) created during the initial cluster deployment. Its main role is to store data of system [tablets](#tablet), which can be considered as cluster-level metadata.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

A static group might require special attention during major maintenance, such as decommissioning an [availability zone](#regions-az).

#### Dynamic group {#dynamic-group}

Regular storage groups that are not [static](#static-group) are called **dynamic groups**. They are called dynamic because they can be created and decommissioned on the fly during [cluster](#cluster) operation.

### Storage pool {#storage-pool}

**Storage pool** is a collection of data storage devices with similar characteristics. Each storage pool is assigned a unique name within a {{ ydb-short-name }} cluster. Technically, each storage pool consists of multiple [PDisks](#pdisk). Each [storage group](#storage-group) is created in a particular storage pool, which determines the performance characteristics of the storage group through the selection of appropriate storage devices. It is typical to have separate storage pools for NVMe, SSD, and HDD devices or particular models of those devices with different capacities and speeds.

### Actor {#actor}

<<<<<<< HEAD
The [actor model](https://en.wikipedia.org/wiki/Actor_model) is one of the main approaches for concurrent programming, which is employed by {{ ydb-short-name }}. In this model, **actors** are lightweight user-space processes that may have and modify their private state but can only affect each other indirectly through message passing. {{ ydb-short-name }} has its own implementation of this model, which is covered [below](#actor-implementation).
=======
The [actor model](https://en.wikipedia.org/wiki/Actor_model) is one of the fundamental approaches to concurrency used in {{ ydb-short-name }}. In this model, **actors** are lightweight user-space processes that can have and modify their private state but can only influence each other indirectly through message passing. {{ ydb-short-name }} has its own implementation of this model, which is described [below](#actor-implementation).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

In {{ ydb-short-name }}, actors with the reliably persisted state are called [tablets](#tablet).

### Tablet {#tablet}

<<<<<<< HEAD
A **tablet** is one of {{ ydb-short-name }}'s primary building blocks and abstractions. It is an entity responsible for a relatively small segment of user or system data. Typically, a tablet manages up to single-digit gigabytes of data, but some kinds of tablets can handle more.

For example, a [row-oriented user table](#row-oriented-table) is managed by one or more [DataShard](#data-shard) tablets, with each tablet responsible for a continuous range of [primary keys](#primary-key) and the corresponding data.

End users sending queries to a {{ ydb-short-name }} cluster aren't expected to know much about tablets, their kinds, or how they work, but it might still be helpful, for example, for performance optimizations.

Technically, tablets are [actors](#actor) with a persistent state reliably saved in [Distributed Storage](#distributed-storage). This state allows the tablet to continue operating on a different [database node](#database-node) if the previous one is down or overloaded.
=======
A **tablet** is one of the fundamental building blocks and abstractions of {{ ydb-short-name }}. It represents an entity responsible for a relatively small segment of user or system data. Typically, a tablet manages up to several gigabytes of data, although some types of tablets can handle larger volumes.

For example, a [row-based user table](#row-oriented-table) is managed by one or more tablets of type [DataShard](#data-shard), with each tablet responsible for a continuous range of [primary keys](#primary-key) and their corresponding data.

End users sending queries to the {{ ydb-short-name }} cluster for execution do not need to know the details of tablets, their types, or how they work, but this knowledge can be useful, for example, for performance optimization.

Technically, tablets are [actors](#actor) with state reliably stored in [distributed storage](#distributed-storage). This state allows the tablet to continue operating on another [database node](#database-node) if the previous one fails or becomes overloaded.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

[Tablet implementation details](#tablet-implementation) and related terms, as well as [main tablet types](#tablet-types), are covered below in the advanced section.

### Transactions {#transactions}

{{ ydb-short-name }} implements **transactions** on two main levels:

<<<<<<< HEAD
* [Local database](#local-database) and the rest of [tablet infrastructure](#tablet-implementation) allow [tablets](#tablet) to manipulate their state using **local transactions** with [serializable isolation level](https://en.wikipedia.org/wiki/Isolation_%28database_systems%29#Serializable). Technically, they aren't really local to a single node as such a state persists remotely in [Distributed Storage](#distributed-storage).
* In the context of {{ ydb-short-name }}, the term **distributed transactions** usually refers to transactions involving multiple tablets. For example, cross-table or even cross-row transactions are often distributed.
* **Single-shard** transactions span a single tablet and are faster to complete. For example, transactions between rows in the same table partition are often single-shard.
=======
* [Local database](#local-database) and the rest of the [tablet infrastructure](#tablet-implementation) allow [tablets](#tablet) to manipulate their state using **local transactions** with [serializable isolation level](https://en.wikipedia.org/wiki/Isolation_(database_systems)#Serializable). Technically, they are not local to a single node, since this state is stored remotely in [distributed storage](#distributed-storage).
* In the context of {{ ydb-short-name }}, the term **distributed transactions** usually refers to transactions that span multiple tablets. For example, transactions between tables or even rows of the same table are often distributed.
* **Single-shard** transactions cover one tablet and execute faster. For example, transactions between rows of the same table partition are often single-shard.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

Together, these mechanisms allow {{ ydb-short-name }} to provide [strict consistency](https://en.wikipedia.org/wiki/Consistency_model#Strict_consistency).

{% if oss %}

The implementation of distributed transactions is covered in a separate article [{#T}](../contributor/datashard-distributed-txs.md), while below there's a list of several [related terms](#deterministic-transactions).

{% endif %}

### Sessions

<<<<<<< HEAD
Logical connections to the database that store the context needed to execute queries and manage transactions. They are explained in more detail in [{#T}](query_execution/index.md#sessions).

### Client-side timeout {#client-timeout}

A **client-side timeout** or **client timeout** is a time limit on how long an application or the {{ ydb-short-name }} SDK waits for a database operation to complete (for example, query execution or receiving a response over gRPC). When this time expires, the client usually stops waiting: it closes the connection or data stream and receives a transport or SDK error — before the server has returned an explicit response (see {{ ydb-short-name }} server response [status codes](../reference/ydb-sdk/ydb-status-codes.md)).

If the client-side timeout is shorter than the query execution time on the {{ ydb-short-name }} side, a query interrupted on the client may continue running on the server for some time due to how queries are processed in the cluster. If this happens at scale, the server becomes overloaded with queries whose responses the client no longer waits for. Therefore, frequently retrying the same query immediately after a timeout can worsen overload. For more information, see [{#T}](../troubleshooting/performance/queries/retry-cascade.md) and [{#T}](../troubleshooting/performance/queries/overloaded-errors.md); SDK retry policies are described in [{#T}](../reference/ydb-sdk/error_handling.md).
=======
Logical connections to the database that store the context needed for executing queries and managing transactions. Sessions are described in more detail in the section [{#T}](query_execution/execution_process.md#sessions).

### Client-side timeout {#client-timeout}

**Client-side timeout** — the time limit that an application or {{ ydb-short-name }} SDK waits for a database operation to complete (for example, executing a query or receiving a response via a gRPC call). After this time expires, the client usually aborts the wait: closes the connection or data stream, receives a transport or SDK error — before the server has had a chance to return an explicit response (see {{ ydb-short-name }} server [response codes](../reference/ydb-sdk/ydb-status-codes.md)).

If the client-side timeout is shorter than the query execution time on the {{ ydb-short-name }} side, then due to the specifics of query processing in the cluster, a query interrupted on the client may continue to execute on the server for some time. If this situation occurs on a large scale, the server becomes overloaded with queries for which the client is not waiting for a response. Therefore, frequent retries of the same query immediately after a timeout can exacerbate the overload. For more details, see the articles [{#T}](../troubleshooting/performance/queries/retry-cascade.md) and [{#T}](../troubleshooting/performance/queries/overloaded-errors.md); retry policies in the SDK are described in the section [{#T}](../reference/ydb-sdk/error_handling.md).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

### Implicit Transactions {#implicit-transactions}

<<<<<<< HEAD
An **implicit transaction** is the query execution mode used when the [transaction mode](transactions.md#modes) is not specified. In this case, {{ ydb-short-name }} automatically determines whether to wrap statements in a transaction. This mode is described in more detail in [{#T}](transactions.md#implicit).

### Multi-version concurrency control {#mvcc}

[**Multi-version concurrency control**](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) or **MVCC** is a method {{ ydb-short-name }} used to allow multiple concurrent transactions to access the database simultaneously without interfering with each other. It is described in more detail in a separate article [{#T}](query_execution/mvcc.md).

### Streaming queries {#streaming-query}

A query type designed for [stream processing](https://en.wikipedia.org/wiki/Stream_processing) of unbounded data. Unlike regular queries, streaming queries have no execution time limit, restart automatically on failures, and periodically persist their state as [checkpoints](#streaming-queries-checkpoints) for fault tolerance.

Streaming queries are described in more detail in [{#T}](streaming-query.md).

### Streaming query checkpoints {#streaming-queries-checkpoints}

Periodically persisted state of a [streaming query](#streaming-query), required to automatically recover execution after failures in a distributed system. For more information about checkpoints, see [{#T}](../dev/streaming-query/checkpoints.md).

### Topology {#topology}

{{ ydb-short-name }} supports several [cluster](#cluster) topologies, described in more detail in a separate article [{#T}](topology.md). A few related terms are explained below.
=======
**Transaction retry** — a client practice of re-executing a [transaction](#transactions) from the beginning upon a retryable error (for example, a temporary network failure or optimistic locking conflict). In {{ ydb-short-name }}, retries should be performed at the transaction level, not at the level of individual queries within it. Built-in retry policies in the {{ ydb-short-name }} SDK and integrations (for example, [spring-ydb-retry](../integrations/spring/spring-retry.md)) implement this approach. For more details, see [{#T}](../reference/ydb-sdk/error_handling.md).

### Exponential backoff {#exponential-backoff}

**Exponential backoff** (also known as **backoff**) — a pause strategy between [transaction retry](#transaction-retry) attempts: the wait interval increases exponentially with each attempt, usually with an upper limit. {{ ydb-short-name }} SDKs often use two levels of backoff — fast and slow — depending on the error type. For more details, see [{#T}](../reference/ydb-sdk/error_handling.md#handling-retryable-errors).

### Jitter {#jitter}

**Jitter** is a small random variation added to the delays of [exponential backoff](#exponential-backoff). It helps avoid simultaneous retries by many clients after a common failure (a "retry storm") and distributes the load more evenly.

### Idempotency {#idempotency}

**Idempotency** is a property of an operation: repeated execution has the same effect as a single execution (for example, `UPSERT` with a deterministic primary key or read operations). [Transaction retries](#transaction-retry) are safe only for idempotent operations or for retry errors where the server guarantees that the transaction was not committed. SDK and client libraries {{ ydb-short-name }} can extend the set of retryable status codes if the calling code marks the operation as idempotent.

### Transaction interceptor {#transaction-interceptor}

**Transaction interceptor** is a Spring Framework component that wraps methods annotated with `@Transactional` and manages transaction boundaries. Modules like [spring-ydb-retry](../integrations/spring/spring-retry.md) replace the standard Spring interceptor, adding [transaction retry](#transaction-retry) logic around transactional methods.

### Implicit transactions {#implicit-transactions}

**Implicit transaction** is a query execution mode where the [transaction mode](transactions.md#modes) is not specified. In this case, {{ ydb-short-name }} independently determines whether to wrap them in a transaction. This mode is described in more detail in [{#T}](transactions.md#implicit).

### Multi-version concurrency control {#mvcc}

[**Multi-version concurrency control**](https://en.wikipedia.org/wiki/Multiversion_concurrency_control), also known as **MVCC**, is a method used by {{ ydb-short-name }} to allow multiple concurrent transactions to access the database without interfering with each other. It is described in more detail in a separate article [{#T}](query_execution/mvcc.md).

### Streaming queries {#streaming-query}

A type of query designed for [stream processing](https://en.wikipedia.org/wiki/Stream_processing) of an unbounded data stream. Unlike regular queries, streaming queries have no limits on execution duration, automatically restart on errors, and periodically save their state as [checkpoints](#streaming-queries-checkpoints) for fault tolerance. [Watermarks](#streaming-queries-watermarks) are used to track processing progress based on event time.

Streaming queries are described in more detail in a separate article [{#T}](./streaming-query/streaming-query.md).

### Streaming query checkpoints {#streaming-queries-checkpoints}

The periodically saved state of a [streaming query](#streaming-query), necessary for automatically restoring its operation after failures in a distributed system. For more details on checkpoints, see the article [{#T}](../dev/streaming-query/checkpoints.md).

### Streaming query watermarks {#streaming-queries-watermarks}

A monotonically increasing lower bound on the event times in a [streaming query](#streaming-query) that may still arrive in the stream. When the watermark reaches value X, the system declares that all events with time less than X have been received with high probability. For more details on watermarks, see the article [{#T}](./streaming-query/watermarks.md).

### Topology {#topology}

{{ ydb-short-name }} supports several **topologies** of a [cluster](#cluster) (or **topology**), described in more detail in a separate article [{#T}](topology.md). Below are explanations of several related terms.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### Availability zones and regions {#regions-az}

An **availability zone** is a data center or an isolated segment thereof with minimal physical distance between nodes and minimal risk of failure at the same time as other availability zones. Thus, availability zones are expected not to share any infrastructure like power, cooling, or external network connections.

<<<<<<< HEAD
A **region** is a large geographic area containing multiple availability zones. The distance between availability zones in the same region is expected to be around 500 km or less. {{ ydb-short-name }} performs synchronous data writes to each availability zone in a region, ensuring reasonable latencies and uninterrupted performance if an availability zone fails.

#### Rack {#rack}

A **rack** or **server rack** is a piece of equipment used to mount multiple servers in an organized manner. Servers in the same rack are more likely to become unavailable simultaneously due to rack-wide issues related to electricity, cooling, etc. Thus, {{ ydb-short-name }} can consider information about which server is located in which rack when placing each piece of data in bare-metal environments.
=======
A **region** is a large geographic area containing multiple availability zones. The distance between availability zones in a single region should be about 500 km or less. {{ ydb-short-name }} performs data writes to each availability zone in the region synchronously, ensuring reasonable latency and uninterrupted operation in case of an availability zone failure.

#### Rack {#rack}

**rack** or **server rack** is equipment used to organize the placement of multiple servers. Servers in the same rack are more likely to become unavailable simultaneously due to rack-level issues related to power, cooling, etc. {{ ydb-short-name }} can take into account information about which server is in which rack when placing each data fragment in environments based on physical servers.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### Pile {#pile}

A **pile** is a set of nodes that can fail or be disconnected simultaneously while other cluster parts (pile) remain operational. A pile can remain operational when other cluster nodes are disconnected. Pile are used in [bridge mode](#bridge) to divide the cluster into several parts with synchronous replication between them. A pile can consist of nodes from one or more regions.

#### Bridge mode {#bridge}

<<<<<<< HEAD
**Bridge mode** is a special cluster topology in which data is stored with synchronous replication between multiple [pile](#pile). Mode details are described in [{#T}](topology.md#bridge) and in [{#T}](bridge.md).
=======
**Bridge mode** is a special cluster topology in which data is stored with synchronous replication between several [piles](#pile). The features of this mode are described in [{#T}](topology.md#bridge) and in [{#T}](bridge.md).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

### Table {#table}

A **table** is a structured piece of information arranged in rows and columns. Each row represents a single record or entry, while each column represents a specific attribute or field with a particular data type.

There are two main approaches to representing tabular data in RAM or on disk drives: [row-oriented (row-by-row)](#row-oriented-table) and [column-oriented (column-by-column)](#column-oriented-table). The chosen approach greatly impacts the performance characteristics of operations with this data, with the former more suitable for transaction workloads (OLTP) and the latter for analytical (OLAP). {{ ydb-short-name }} supports both.

#### Row-oriented table {#row-oriented-table}

<<<<<<< HEAD
**Row-oriented tables** store data for all or most columns of a given row physically close to each other. They are explained in more detail in [{#T}](datamodel/table.md#row-oriented-tables).
=======
**Row-oriented tables** store data for all or most columns of each row physically close together. They are described in more detail in [{#T}](datamodel/table.md#row-oriented-tables).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### Column-oriented table {#column-oriented-table}

**Column-oriented tables** or **columnar tables** store data for each column independently. They are optimized for building aggregates over a small number of columns but are less suitable for accessing particular rows, as rows need to be reconstructed from their cells on the fly. They are explained in more detail in [{#T}](datamodel/table.md#column-oriented-tables).

#### Primary key {#primary-key}

<<<<<<< HEAD
A **primary key** is an ordered list of columns, the values of which uniquely identify rows. It is used to build the [table's primary index](#primary-index). It is provided by the {{ ydb-short-name }} user during [table creation](../yql/reference/syntax/create_table/index.md) and dramatically impacts the performance of workloads interacting with that table.

The guidelines on choosing primary keys are provided in [{#T}](../dev/primary-key/index.md).
=======
**Primary key** is an ordered list of columns whose values uniquely identify a row. It is used to create the table's [primary index](#primary-index). It is set by the {{ ydb-short-name }} user when [creating a table](../yql/reference/syntax/create_table/index.md) and significantly affects the performance of operations on that table.

Guidance on choosing primary keys is provided in [{#T}](../dev/primary-key/index.md).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### Primary index {#primary-index}

A **primary index** or **primary key index** is the main data structure used to locate rows in a table. It is built based on the chosen [primary key](#primary-key) and determines the physical order of rows in a table; thus, each table can have only one primary index. The primary index is unique.

#### Secondary index {#secondary-index}

<<<<<<< HEAD
A **secondary index** is an additional data structure used to locate rows in a table, typically when it can't be done efficiently using the [primary index](#primary-index). Unlike the primary index, secondary indexes are managed independently from the main table data. Thus, a table might have multiple secondary indexes for different use cases. {{ ydb-short-name }}'s capabilities in terms of secondary indexes are covered in a separate article [Secondary indexes](query_execution/secondary_indexes.md). Secondary indexes can be either unique or non-unique.

A special type of **secondary index** is singled out separately - [vector index](#vector-index) and [fulltext index](#fulltext-index).
=======
**Secondary index** is an additional data structure used to find rows in a table, typically when this cannot be done efficiently using the [primary index](#primary-index). Unlike the primary index, secondary indexes are managed independently of the table's main data. Thus, a table can have multiple secondary indexes for different scenarios. {{ ydb-short-name }} capabilities regarding secondary indexes are described in a separate article [{#T}](query_execution/secondary_indexes.md). A secondary index can be either unique or non-unique.

Special types of secondary indexes are distinguished separately: [vector index](#vector-index), [full-text index](#fulltext-index), and [JSON index](#json-index).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### Vector Index {#vector-index}

<<<<<<< HEAD
**Vector index** is an additional data structure used to speed up the [vector search](query_execution/vector_search.md) when there is a large amount of data, and the [exact vector search without an index](../yql/reference/udf/list/knn.md) does not perform satisfactorily.
The capabilities of {{ ydb-short-name }} regarding **ANN search** (approximate nearest neighbor search) with vector indexes are described in a separate article [{#T}](../dev/vector-indexes.md).

**Vector index** is a specialized type of [secondary index](#secondary-index) designed for similarity-based searching, which differs from traditional secondary indexes that optimize for equality or range queries.
=======
A **vector index** is an additional data structure used to speed up the [vector search](query_execution/vector_search.md) problem when there is a large amount of data and [exact vector search without an index](../yql/reference/udf/list/knn.md) does not work satisfactorily.
The capabilities of {{ ydb-short-name }} for approximate nearest neighbor search (ANN search) using vector indexes are described in a separate article [{#T}](../dev/vector-indexes.md).

**Vector index** is a specialized type of [secondary index](#secondary-index) designed for similarity search, unlike traditional secondary indexes, which are optimized for equality or range searches.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### Fulltext index {#fulltext-index}

**Fulltext index** is an additional data structure used to speed up text search in a table column by words and phrases (and, with n-grams, by substrings).

<<<<<<< HEAD
The fulltext search capabilities and index parameters are described in [{#T}](../dev/fulltext-indexes.md) and [{#T}](query_execution/fulltext_search.md).

#### Local index {#local-index}

A local index is an auxiliary structure stored together with table data (unlike a [global secondary index](#secondary-index), which materializes a separate index table). It is applied while reading the main table in storage. See [local indexes](query_execution/local_indexes.md).
=======
The full-text search capabilities and index parameters are described in the articles [{#T}](../dev/fulltext-indexes.md) and [{#T}](query_execution/fulltext_search.md).

#### JSON index {#json-index}

**JSON index** is an additional data structure used to speed up predicates with the [JSON_EXISTS](../yql/reference/builtins/json.md#json_exists) and [JSON_VALUE](../yql/reference/builtins/json.md#json_value) functions on a column of type `Json` or `JsonDocument`. Unlike traditional secondary indexes optimized for equality or range searches on individual table columns, the JSON index works with arbitrary [JsonPath](../yql/reference/builtins/json.md#jsonpath) paths within a JSON document.

A JSON index, like a [full-text index](#fulltext-index), is built on top of an [inverted index](https://en.wikipedia.org/wiki/Inverted_index), but uses its own JSON document tokenizer. JSON search capabilities are described in the articles [{#T}](../dev/json-indexes.md) and [{#T}](query_execution/json_search.md).

#### Local index {#local-index}

A local index is an auxiliary structure that is stored together with the table data (unlike a [global secondary index](#secondary-index), which materializes a separate index table). A local index is used when reading the main table on the storage side. For more information, see [local indexes](query_execution/local_indexes.md).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### Bloom filter {#bloom-filter}

A Bloom filter is a [probabilistic data structure](https://en.wikipedia.org/wiki/Bloom_filter) for testing set membership. It may produce false positives, but there are no false negatives.

#### Local Bloom skip index {#local-bloom-skip-index}

<<<<<<< HEAD
A local Bloom skip index is a kind of [local index](#local-index): a probabilistic column-value filter based on a [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) that speeds up selective queries by skipping data fragments that cannot contain the requested value. See [Bloom skip indexes](../dev/bloom-skip-indexes.md) and [local indexes](query_execution/local_indexes.md).
=======
A Local Bloom index is a special case of a [local index](#local-index): a probabilistic filter based on column values using a [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter), which speeds up selective queries by skipping data fragments where the searched value is guaranteed to be absent. For more information, see [Bloom indexes](../dev/bloom-skip-indexes.md), [local indexes](query_execution/local_indexes.md).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### Column family {#column-family}

A **column family** or **column group** is a feature that allows storing a subset of [row-oriented table](#row-oriented-table) columns separately in a distinct family or group. The primary use case is to store some columns on different kinds of disk drives (offload less important columns to HDD) or with various compression settings. If the workload requires many column families, consider using [column-oriented tables](#column-oriented-table) instead.

#### Column encoding {#column-encoding}

**Column encoding** is a mechanism for optimizing data storage in table columns that reduces disk usage and can speed up some operations.

#### Time to Live {#ttl}

**Time to live** or **TTL** is a mechanism for automatically removing old rows from a table asynchronously in the background. It is explained in a separate article [{#T}](ttl.md).

### View {#view}

<<<<<<< HEAD
A **view** is a way to save a query and access its results as if they were a real table. The view itself stores no data except the query text. The query stored in the view is executed on every SELECT from it, generating the returned result. Any changes in the tables referenced by the view are immediately reflected in read results from it.
=======
A **view** is a way to save a query and access its results as if they were a real table. The view itself does not store any data except the query text. The query stored in the view is executed each time a SELECT is run against it, generating the returned result. Any changes to the tables referenced by the view are immediately reflected in the results read from it.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

{% if feature_view %}

There are user-defined and system-defined views.

#### User-defined view {#user-view}

**User-defined views** are created by a user with the [{#T}](../yql/reference/syntax/create-view.md) statement. They are described in more detail in [{#T}](../concepts/datamodel/view.md).

{% endif %}

#### System view {#system-view}

<<<<<<< HEAD
**System views** are special views automatically created by the system for monitoring the state of the database and cluster. They are located in a special directory `.sys` in the root folder of each database. System views for databases are described in [{#T}](../dev/system-views.md); system views for the cluster, as well as access control issues for them, are described in [{#T}](../devops/observability/system-views.md).

### Topic {#topic}

A **topic** is a persistent queue that can be used for reliable asynchronous communications between various systems via message passing. {{ ydb-short-name }} provides the infrastructure to ensure "exactly once" semantics in such communications, which ensures that there are both no lost messages and no accidental duplicates.

Several terms related to topics are listed below. How {{ ydb-short-name }} topics work is explained in more detail in a separate article [{#T}](datamodel/topic.md).
=======
**System views** are special views automatically created by the system for monitoring the state of a database and cluster. They are located in the special directory `.sys`, which is in the root folder of each database. System views for databases are described in [{#T}](../dev/system-views.md); system views for the cluster, as well as access control issues, are described in [{#T}](../devops/observability/system-views.md).

### Topic {#topic}

**Message queue** is used for reliable asynchronous communication between different systems by means of message passing. {{ ydb-short-name }} provides infrastructure that ensures "exactly once" semantics in such communications. Using it, you can achieve guarantees of no lost messages and no accidental duplicates.

**Topic** is a named entity in a message queue, designed for interaction between [writers](#producer) and [readers](#consumer).

Several terms related to topics are given below. How topics work in {{ ydb-short-name }} is explained in more detail in a separate article [{#T}](datamodel/topic.md).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### Partition {#partition}

For horizontal scaling purposes, topics are divided into separate elements called **partitions**. Thus, a partition is a unit of parallelism within a topic. Messages inside each partition are ordered.

However, subsets of data managed by a single [data shard](#data-shard) or [column shards](#column-shard) can also be called partitions.

#### Offset {#offset}

An **offset** is a sequence number that identifies a message inside a [partition](#partition).

#### Producer {#producer}

<<<<<<< HEAD
A **producer** is an entity that writes new messages to a topic.
=======
**Writer** or **producer** is an entity that writes new messages to a topic.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### Consumer {#consumer}

<<<<<<< HEAD
A **consumer** is an entity that reads messages from a topic.

### Change data capture {#cdc}

**Change data capture** or **CDC** is a mechanism that allows subscribing to a **stream of changes** to a given [table](#table). Technically, it is implemented on top of [topics](#topic). It is described in more detail in a separate article [{#T}](cdc.md).

#### Changefeed {#changefeed}

**Changefeed** or **stream of changes** is an ordered list of changes in a given [table](#table) published via a [topic](#topic).

### Backup collection {#backup-collection}

A **backup collection** is a [schema object](#scheme-object) that organizes full and incremental [backups](#backup) for selected [row-oriented tables](#row-oriented-table). Collections enable recovery to any saved backup point in the chain by maintaining [backup chains](#backup-chain) and ensuring consistent restoration across multiple tables. A table can only belong to one backup collection at a time.
=======
**Reader** or **consumer** is an entity that reads messages from a topic.

### Change data capture {#cdc}

**Change data capture** or **CDC** is a mechanism that allows subscribing to a **change stream** on a specific [table](#table). Technically, it is implemented on top of [topics](#topic). It is described in more detail in a separate article [{#T}](cdc.md).

#### Change stream {#changefeed}

**Change stream** is an ordered list of changes to a [table](#table), placed in a [topic](#topic).

### Backup collection {#backup-collection}

**Backup collection** is a [schema object](#scheme-object) that organizes full and incremental [backups](#backup) for selected [row tables](#row-oriented-table). Collections provide [point-in-time recovery](https://en.wikipedia.org/wiki/Point-in-time_recovery), maintaining [backup chains](#backup-chain) and ensuring consistent recovery of multiple tables. A table can belong to only one backup collection at a time.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

For more information, see [{#T}](datamodel/backup-collection.md).

#### Backup {#backup}

A **backup** is a copy of data at a specific point in time that can be used to restore the data. In the context of [backup collections](#backup-collection), there are two types:

<<<<<<< HEAD
- **Full backup**: A complete snapshot of all data in the collection. Serves as the foundation for [backup chains](#backup-chain) and can be restored independently.
=======
- **Full backup**: A complete snapshot of all data in the collection. Serves as the basis for [backup chains](#backup-chain) and can be restored independently.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))
- **Incremental backup**: Captures only changes (inserts, updates, deletes) since the previous backup. Requires the entire backup chain for restoration.

#### Backup chain {#backup-chain}

<<<<<<< HEAD
A **backup chain** is an ordered sequence of [backups](#backup) starting with a full backup followed by zero or more incremental backups. Each incremental backup depends on all previous backups in the chain. Deleting any backup in the chain makes subsequent incremental backups unrestorable.

{% if feature_async_replication == true %}

### Asynchronous replication instance {#async-replication-instance}

An **asynchronous replication instance** is a named entity that stores [asynchronous replication](async-replication.md) settings (connection properties, a list of replicated objects, etc.). It can also be used to retrieve the status of asynchronous replication, such as the [initial synchronization process](async-replication.md#initial-scan), [replication lag](async-replication.md#replication-of-changes), [errors](async-replication.md#error-handling), and more.

#### Replicated object {#replicated-object}

A **replicated object** is an object (for example, a table) for which asynchronous replication is configured.

#### Replica object {#replica-object}

A **replica object** is a mirror copy of the replicated object, automatically created by an asynchronous replication instance. As a rule, it is read-only.
=======
**Backup chain** is an ordered sequence of [backups](#backup), starting with a full backup followed by zero or more incremental backups. Each incremental backup depends on all previous backups in the chain. Deleting any backup in the chain makes subsequent incremental backups unrecoverable.

{% if feature_async_replication == true %}

### Async replication instance {#async-replication-instance}

**Async replication instance** is a named entity that stores the settings of [asynchronous replication](async-replication.md) (connection settings, list of replicated objects, etc.). It can also be used to obtain information about the state of asynchronous replication: [initial scan progress](async-replication.md#initial-scan), [lag](async-replication.md#replication-of-changes), [errors](async-replication.md#error-handling), etc.

#### Replicated object {#replicated-object}

**Replicated object** is an object (e.g., a table) for which asynchronous replication is configured.

#### Replica object {#replica-object}

**Replica object** is a "mirror copy" of the replicated object, automatically created by the async replication instance. Typically, it is read-only.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

{% endif %}

{% if feature_transfer == true %}

### Transfer instance {#transfer-instance}

<<<<<<< HEAD
A **transfer instance** is a named entity that stores [transfer](transfer.md) settings, including connection settings and data transformation rules. It can also be used to retrieve transfer status information, such as [errors](transfer.md#error-handling).
=======
**Transfer instance** is a named entity that stores the settings of a [transfer](transfer.md), including connection settings and data transformation rules. It can also be used to obtain information about the transfer state, such as [errors](transfer.md#error-handling).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

{% endif %}

### Coordination node {#coordination-node}

<<<<<<< HEAD
A **coordination node** is a schema object that allows client applications to create semaphores for coordinating their actions. Learn more about [coordination nodes](./datamodel/coordination-node.md).

#### Semaphore {#semaphore}

A **semaphore** is an object within a [coordination node](#coordination-node) that provides a synchronization mechanism for distributed applications. Semaphores can be persistent or ephemeral and support operations like creation, acquisition, release, and monitoring. Learn more about [semaphores in {{ ydb-short-name }}](./datamodel/coordination-node.md#semaphore).
=======
**Coordination node** is a schema object that allows client applications to create semaphores for coordinating their actions. Coordination nodes are used to implement distributed locks, service discovery, leader election, and other scenarios. For more details, see [coordination nodes](./datamodel/coordination-node.md).

#### Semaphore {#semaphore}

**Semaphore** is an object inside a [coordination node](#coordination-node) that provides a synchronization mechanism for distributed applications. Semaphores can be permanent or temporary and support create, acquire, release, and monitor operations. For more details, see [semaphores in {{ ydb-short-name }}](./datamodel/coordination-node.md#semaphore).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

{% if feature_resource_pool == true and feature_resource_pool_classifier == true %}

### Resource pool {#resource-pool}

<<<<<<< HEAD
A **resource pool** is a schema object that describes the restrictions placed on the resources (CPU, RAM, etc.) available for executing queries in that pool. A query is always executed in some resource pool. By default, all queries run in a resource pool named `default`, which does not impose any restrictions. For more on using resource pools, see [{#T}](../dev/resource-consumption-management.md).

### Resource pool classifier {#resource-pool-classifier}

A **resource pool classifier** is an object used to control how queries are distributed across [resource pools](#resource-pool). It defines the rules by which a resource pool is chosen for each query. These classifiers are global to the entire [database](#database) and apply to all queries submitted to it. For more on how they are used, see [{#T}](../dev/resource-consumption-management.md).
=======
**Resource pool** is a schema object that describes the limits imposed on resources (CPU, RAM, etc.) available for executing queries in this resource pool. A query is always executed in some resource pool. By `default`, all queries are executed in a resource pool named , which imposes no restrictions. For more details on using resource pools, see the article [{#T}](../dev/resource-consumption-management.md).

### Resource pool classifier {#resource-pool-classifier}

**Resource pool classifier** is an object designed to manage the distribution of queries among [resource pools](#resource-pool). It describes the rules by which a resource pool is selected for each query. These classifiers are global for the entire [database](#database) and apply to all queries entering it. For more details on their usage, see the article [{#T}](../dev/resource-consumption-management.md).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

{% endif %}

### YQL {#yql}

<<<<<<< HEAD
**YQL ({{ ydb-short-name }} Query Language)** is a high-level language for working with the system. It is a dialect of [ANSI SQL](https://en.wikipedia.org/wiki/SQL). There's a lot of content covering YQL, including a [tutorial](../dev/yql-tutorial/index.md), [reference](../yql/reference/syntax/index.md), and [recipes](../yql/reference/recipes/index.md).

### Federated queries {#federated-queries}

**Federated queries** is a feature that allows querying data stored in systems external to the {{ ydb-short-name }} cluster.
=======
**YQL ({{ ydb-short-name }} Query Language)** is a high-level language for working with the system. It is a dialect of [ANSI SQL](https://en.wikipedia.org/wiki/SQL). There are many materials dedicated to YQL, including a [tutorial](../dev/yql-tutorial/index.md), [reference guide](../yql/reference/syntax/index.md), and [recipes](../yql/reference/recipes/index.md).

### Federated queries {#federated-queries}

**Federated queries** is a feature that allows executing queries against data stored in systems external to the {{ ydb-short-name }} cluster.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

A few terms related to federated queries are listed below. How {{ ydb-short-name }} federated queries work is explained in more detail in a separate article [Federated query](query_execution/federated_query/index.md).

#### External data source {#external-data-source}

<<<<<<< HEAD
An **external data source** or **external connection** is a piece of metadata that describes how to connect to a supported external system for [federated query execution](#federated-queries).

#### External table {#external-table}

An **external table** is a piece of metadata that describes a particular dataset that can be retrieved from an [external data source](#external-data-source).

#### Secret {#secret}

A **secret** is a sensitive piece of metadata that requires special handling. For example, secrets can be used in [external data source](#external-data-source) definitions and represent things like passwords and tokens.
=======
**External data source** or **external connection** is metadata that describes how to connect to a supported external system to execute [federated queries](#federated-queries).

#### External table {#external-table}

**External table** is metadata that describes a specific data set that can be retrieved from an [external data source](#external-data-source).

#### Secret {#secret}

**Secret** is confidential metadata that requires special handling. For example, secrets can be used in definitions of [external data sources](#external-data-source) and represent entities such as passwords and tokens.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

### Auth token {#auth-token}

<<<<<<< HEAD
An **authentication token** or **auth token** is a token that {{ ydb-short-name }} uses for [authentication](../security/authentication.md).
=======
**Auth token** is a token used for [authentication](../security/authentication.md) in {{ ydb-short-name }}.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

{{ ydb-short-name }} supports various [authentication modes](../security/authentication.md) and token types.

### Cluster scheme {#scheme}

<<<<<<< HEAD
A **{{ ydb-short-name }} cluster scheme** is a hierarchical namespace of a {{ ydb-short-name }} cluster. The top-level element of the namespace is the [cluster scheme root](#scheme-root) that contains [databases](#database) as its children. Scheme objects inside databases can use nested directories to form a hierarchy.
=======
**mTLS** (mutual TLS) is a [TLS](https://en.wikipedia.org/wiki/Transport_Layer_Security) mode in which not only does the client verify the server certificate, but the server also requests and verifies the client's [client certificate](#client-certificate) when establishing a connection.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

### Database scheme {#scheme-database}

<<<<<<< HEAD
A **database scheme** is a subset of the hierarchical namespace of a {{ ydb-short-name }} cluster that belongs to a database.

### Database root {#scheme-database-root}

A **database root** is a path to a database in a {{ ydb-short-name }} cluster scheme.
=======
A **client certificate** is a [digital certificate](https://en.wikipedia.org/wiki/X.509) issued and used by a client — an application, user, or [{{ ydb-short-name }} node](#node) — to confirm its identity when interacting with {{ ydb-short-name }}.

### Cluster schema {#scheme}

The **{{ ydb-short-name }} cluster schema** is the hierarchical namespace of the {{ ydb-short-name }} cluster. The top-level element of this namespace is the [cluster schema root](#scheme-root). The child elements of the cluster schema root are [databases](#database). Inside each database, you can create an arbitrary hierarchy of [objects](#scheme-object) (tables, topics, etc.) using nested directories.

### Database schema {#scheme-database}

A **database schema** is a subset of the cluster's hierarchical namespace that belongs to a database.

### Database root {#scheme-database-root}

The **database root** is the path to the database in the cluster schema.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

### Scheme root {#scheme-root}

<<<<<<< HEAD
A **scheme root** is a root element of a [{{ ydb-short-name }} cluster scheme](datamodel/index.md#cluster-scheme). Children elements of the cluster scheme root can be [databases](#database) or other [scheme objects](#scheme-object).
=======
The **cluster schema root** is the root element of the [{{ ydb-short-name }} namespace](datamodel/cluster-namespace.md), whose child elements are [databases](#database).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

### Scheme object {#scheme-object}

<<<<<<< HEAD
A database schema consists of **scheme objects**, which can be databases, [tables](#table) (including [external tables](#external-table)), [topics](#topic), [folders](#folder), and so on.
=======
A database schema consists of **schema objects**, which can be databases, [tables](#table) (including [external tables](#external-table)), [topics](#topic), [folders](#folder), etc.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

For organizational convenience, scheme objects form a hierarchy using [folders](#folder).

### Folder {#folder}

As in file systems, a **folder** or **directory** is a container for [scheme objects](#scheme-object).

Folders can contain subfolders, and this nesting can have arbitrary depth.

### Access object {#access-object}

<<<<<<< HEAD
An **access object** in the context of [authorization](../security/authorization.md) is an entity for which access rights and restrictions are configured. In {{ ydb-short-name }}, access objects are [scheme objects](#scheme-object).
Each access object has an [owner](#access-owner) and an [access control list](#access-control-list).

### Access subject {#access-subject}

An **access subject** is an entity that can interact with [access objects](#access-object) or perform specific actions within the system. Access to these interactions and actions depends on configured [access control lists](#access-control-list).
=======
An **access object** in [authorization](../security/authorization.md) is an entity for which access rights and restrictions are configured. In {{ ydb-short-name }}, access objects are [schema objects](#scheme-object).

Each [schema object](#scheme-object) has an [owner](#access-owner) and an [access control list](#access-control-list) on that object, granted to users and groups ([access subjects](#access-subject)).

### Access subject {#access-subject}

An **access subject** is an entity that can access [access objects](#access-object) and perform certain actions in the system.

Obtaining access during these requests and actions depends on the configured [access control lists](#access-control-list) and the subject's [access level](#access-level).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

An access subject can be a [user](#access-user) or a [group](#access-group).

### Access right {#access-right}

An **[access right](../security/authorization.md#right)** is an entity that represents permission for an [access subject](#access-subject) to perform a specific set of operations in a cluster or database on a specific [access object](#access-object).

### Access right inheritance {#access-right-inheritance}

**Access rights inheritance** is a mechanism by which [access rights](#access-right) granted on parent [access objects](#access-object) are inherited by child objects in the hierarchical structure of the database. This ensures that permissions granted at a higher level in the hierarchy are applied to all sublevels beneath it, unless [explicitly overridden](../reference/ydb-cli/commands/scheme-permissions.md#clear-inheritance).

### Access control list {#access-control-list}

<<<<<<< HEAD
An **access control list** or **ACL** is a list of all [rights](#access-right) granted to [access subjects](#access-subject) (users and groups) for a specific [access object](#access-object).
=======
An **[access control list](../security/authorization.md#right)** or **ACL** is a list of all [rights](#access-right) granted to [access subjects](#access-subject) (users and groups) on a specific [access object](#access-object).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

### Access level {#access-level}

An **access level** determines additional privileges of an [access subject](#access-subject) for [scheme objects](#scheme-object) as well as privileges that are not related to [scheme objects](#scheme-object).

<<<<<<< HEAD
{{ ydb-short-name }} uses hierarchical access levels:
=======
- Database
- Viewer
- Monitoring
- Administration.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

- Database;
- Viewer;
- Monitoring;
- Administration.

An access level is granted by adding an access subject to an [access level list](#access-level-list).

### Access level list {#access-level-list}

<<<<<<< HEAD
An **access level list** is a list of [SIDs](#access-sid) that grants a certain [access level](#access-level) to the associated [access subjects](#access-subject).
=======
An **access level list** or **permission list** is a list of [SID](#access-sid)s of [access subjects](#access-subject) that are allowed a specific [access level](#access-level).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

{{ ydb-short-name }} provides several [access level lists](../reference/configuration/security_config.md#security-access-levels) that collectively determine [access levels](#access-level) in the system.

<<<<<<< HEAD
For detailed information about access level lists, their hierarchy, and how they work, see [Access level lists](../security/authorization.md#access-level-lists) in the Authorization documentation.
=======
For details on access control lists, their hierarchy, and how they work, see the [Access Control Lists](../security/authorization.md#access-level-lists) section of the authorization documentation.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

### Owner {#access-owner}

An **[owner](../security/authorization.md#owner)** is an [access subject](#access-subject) ([user](#access-user) or [group](#access-group)) having full rights over a specific [access object](#access-object).

### User {#access-user}

A **[user](../security/authorization.md#user)** is an individual utilizing {{ ydb-short-name }} to perform a specific function.

{{ ydb-short-name }} has the following types of users depending on their source:

- local users in {{ ydb-short-name }} databases
<<<<<<< HEAD
- external users from third-party directory services
=======
- external users from third-party directories
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

{{ ydb-short-name }} users are identified by their [SIDs](#access-sid).

#### Local user {#local-user}

A **local user** is an individual whose {{ ydb-short-name }} account is created directly in {{ ydb-short-name }} using the `CREATE USER` command or during the [initial security configuration](../security/builtin-security.md).

#### External user {#external-user}

An **external user** is an individual whose {{ ydb-short-name }} account is created in a third-party directory service, for example, in LDAP or IAM.

### Group {#access-group}

A **[group](../security/authorization.md#group)** or **access group** is a named collection of [users](#access-user) with identical [access rights](#access-right) to certain [access objects](#access-object).

### Role {#access-role}

A **role** is a named collection of [access rights](#access-right) that can be granted to [users](#access-user) or [groups](#access-group).

<<<<<<< HEAD
Roles in {{ ydb-short-name }} are implemented as [groups](#access-group) that are created during the initial cluster deployment and granted a set of [access rights](#access-right) on the root of the cluster scheme.
=======
Roles in {{ ydb-short-name }} are implemented using [groups](#access-group) that are created during the initial cluster deployment and are assigned a specific [access list](#access-right) on the cluster schema root. For more information about roles, see [{#T}](../security/builtin-security.md).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

### SID {#access-sid}

**SID** (**Security Identifier**) is a string in the format `<login>[@<subsystem>]`, identifying an [access subject](../concepts/glossary.md#access-subject) in [access control lists](#access-control-list).

<<<<<<< HEAD
### Query optimizer {#optimizer}
=======
An SID identifies an individual [user](#access-user) or [user group](#access-group).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

[**Query optimizer**](https://en.wikipedia.org/wiki/Query_optimization) is a {{ ydb-short-name }} component that takes a logical plan as input and produces the most efficient physical plan with the lowest estimated resource consumption among the alternatives. The {{ ydb-short-name }} query optimizer is described in the [{#T}](query_execution/optimizer.md) section.

<<<<<<< HEAD
## Advanced terminology {#advanced-terminology}

This section explains terms that are useful to [{{ ydb-short-name }} contributors](../contributor/index.md) and users who want to get a deeper understanding of what's going on inside the system.

### Actors implementation {#actor-implementation}
=======
### Query optimizer {#optimizer}

[**Query optimizer**](https://en.wikipedia.org/wiki/Query_optimization) is a set of {{ ydb-short-name }} components responsible for converting the logical representation of a query into a specific physically executable plan to obtain the requested result. The main goal of the optimizer is to select, among all possible query execution plans, one that is sufficiently efficient in terms of predicted execution time and cluster resource consumption. It is described in more detail in a separate article [{#T}](query_execution/optimizer.md).

### Compilation cache {#compile-cache}
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### Actor system {#actor-system}

<<<<<<< HEAD
An **actor system** is a C++ library with {{ ydb-short-name }}'s [implementation](https://github.com/ydb-platform/ydb/tree/main/ydb/library/actors) of the [Actor model](https://en.wikipedia.org/wiki/Actor_model).

#### Actor service {#actor-service}

An **actor service** is an [actor](#actor) that has a well-known name and is usually run in a single instance on a [node](#node).
=======
## Advanced terminology {#advanced-terminology}

This section explains terms that are useful for [{{ ydb-short-name }} contributors](../contributor/index.md) and users who want to understand more deeply what happens inside the system.

### Actor implementation {#actor-implementation}

#### Actor system {#actor-system}

**Actor system** is a C++ library with an [implementation](https://github.com/ydb-platform/ydb/tree/main/ydb/library/actors) of the [actor model](https://en.wikipedia.org/wiki/Actor_model) for {{ ydb-short-name }} needs.

#### Actor service {#actor-service}

**Actor service** is an [actor](#actor) that has a well-known name and typically runs as a single instance on a [node](#node).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### ActorId {#actorid}

An **ActorId** is a unique identifier of the actor or [tablet](#tablet) in the [cluster](#cluster).

#### Actor system interconnect {#actor-system-interconnect}

<<<<<<< HEAD
The **actor system interconnect** or **interconnect** is the [cluster's](#cluster) internal network layer. All [actors](#actor) interact with each other within the system via the interconnect.
=======
**Actor system interconnect**, **interconnect** is the internal network layer of a [cluster](#cluster). All [actors](#actor) communicate with each other in the system through the interconnect.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### Local {#local}

A **Local** is an [actor service](#actor-service) running on each [node](#node). It directly manages the [tablets](#tablet) on its node and interacts with [Hive](#hive). It registers with Hive and receives commands to launch tablets.

#### Actor system pool {#actor-system-pool}

The **actor system pool** is a [thread pool](https://en.wikipedia.org/wiki/Thread_pool) used to run [actors](#actor). Each [node](#node) operates multiple pools to coarsely separate resources between different types of activities. A typical set of pools includes:

- **System**: A pool that handles internal operations within {{ ydb-short-name }} node. It serves system [tablets](#tablet), [state storage](#state-storage), [distributed storage](#distributed-storage) I/O, and so on.

- **User**: A pool dedicated to user-generated load, such as running non-system tablets or queries executed by the [QP](#kqp).

- **Batch**: A pool for tasks without strict execution deadlines, including heavy queries handled by the [QP](#kqp) background operations like backups, data compaction, and garbage collection.

- **IO**: A pool for tasks involving blocking operations, such as authentication or writing logs to files.

- **IC**: A pool for [interconnect](#actor-system-interconnect), responsible for system calls related to data transfers across the network, data serialization, message splitting and merging.

### Tablet implementation {#tablet-implementation}

<<<<<<< HEAD
A [**tablet**](#tablet) is an [actor](#actor) with a persistent state. It includes a set of data for which this tablet is responsible and a finite state machine through which the tablet's data (or state) changes. The tablet is a fault-tolerant entity because tablet data is stored in a [Distributed storage](#distributed-storage) that survives disk and node failures. The tablet is automatically restarted on another [node](#node) if the previous one is down or overloaded. The data in the tablet changes in a consistent manner because the system infrastructure ensures that there is no more than one [tablet leader](#tablet-leader) through which changes to the tablet data are carried out.

The tablet solves the same problem as the [Paxos](https://en.wikipedia.org/wiki/Paxos_(computer_science)) and [Raft](https://en.wikipedia.org/wiki/Raft_(algorithm)) algorithms in other systems, namely the [distributed consensus](https://en.wikipedia.org/wiki/Consensus_(computer_science)) task. From a technical point of view, the tablet implementation can be described as a Replicated State Machine (RSM) over a shared log, as the tablet state is completely described by an ordered command log stored in a distributed and fault-tolerant storage.

During execution, the tablet state machine is managed by three components:

1. The generic tabular part ensures the log's consistency and recovery in case of failures.
2. **Executor** is an abstraction of a local database, namely data structures and code that arrange work with the data stored by the tablet.
3. An [actor](#actor) with a custom code that implements the specific logic of a specific tablet type.
=======
A [**tablet**](#tablet) is an [actor](#actor) with persistent state. It includes a set of data that the tablet is responsible for, and a state machine through which the tablet's data (or state) is modified. A tablet is a fault-tolerant entity because its data is stored in [distributed storage](#distributed-storage), which survives disk and node failures. A tablet automatically restarts on another [node](#node) if the previous one fails or becomes overloaded. Data in a tablet is modified sequentially, as the system infrastructure guarantees that there is no more than one [tablet leader](#tablet-leader) through which tablet data changes are performed.

A tablet solves the same problem as the [Paxos](https://en.wikipedia.org/wiki/Paxos_(computer_science)) and [Raft](https://en.wikipedia.org/wiki/Raft_(algorithm)) algorithms in other systems, namely the problem of [distributed consensus](https://en.wikipedia.org/wiki/Consensus_(computer_science)). From a technical standpoint, a tablet implementation can be described as a replicated state machine (RSM) on top of a shared log, since the tablet state is fully described by an ordered log of commands stored in a distributed and fault-tolerant storage.

At runtime, the tablet state machine is managed by three components:

1. The common tablet part ensures log consistency and recovery in case of failures.
2. The **executor** is an abstraction of a local database, namely the data structures and code that organize work with the data stored by the tablet.
3. An actor with user code that implements the specific logic of a particular tablet type.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

In {{ ydb-short-name }}, there are multiple kinds of specialized tablets storing all kinds of data for all sorts of tasks. Many {{ ydb-short-name }} features like [tables](#table) and [topics](#topic) are implemented as specific tablets. Thus, reusing tablet infrastructure is one of the key means of {{ ydb-short-name }} extensibility as a platform.

<<<<<<< HEAD
Usually, there are orders of magnitude more tablets running in a {{ ydb-short-name }} cluster compared to processes or threads that other systems would use for a similarly sized cluster. There can easily be hundreds of thousands to millions of tablets working simultaneously in a {{ ydb-short-name }} cluster.

Since the tablet stores its state in [Distributed storage](#distributed-storage), it can be (re)started on any node of the cluster. Tablets are identified using [TabletID](#tabletid), a 64-bit number assigned when creating a tablet.

### Tablet leader {#tablet-leader}

A **tablet leader** is the current active leader of a given tablet. The tablet leader accepts commands, assigns them an order, and confirms them to the outside world. It is guaranteed that there is no more than one leader for a given tablet at any moment.

### Tablet candidate {#tablet-candidate}

A **tablet candidate** is one of the election participants who wants to become a [leader](#tablet-leader) for a given tablet. If a candidate wins the election, it assumes the tablet leader role.
=======
Typically, a {{ ydb-short-name }} cluster runs orders of magnitude more tablets compared to the processes or threads that other systems would use for a cluster of similar size. A {{ ydb-short-name }} cluster can easily have hundreds of thousands or millions of tablets running simultaneously.

Since a tablet stores its state in [distributed storage](#distributed-storage), it can be (re)started on any node of the cluster. Tablets are identified by a [TabletID](#tabletid), a 64-bit number assigned when the tablet is created.

### Tablet leader {#tablet-leader}

A **tablet leader** is the current active leader of a given tablet. The tablet leader accepts commands, assigns them an order, and confirms them to the outside world. It is guaranteed that at any given time there is no more than one leader for each tablet.

### Tablet candidate {#tablet-candidate}

A **tablet candidate** is one of the election participants that wants to become the [leader](#tablet-leader) of a given tablet. If the candidate wins the election, it becomes the tablet leader.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

### Tablet follower {#tablet-follower}

<<<<<<< HEAD
A **tablet follower** or **hot standby** is a copy of a [tablet leader](#tablet-leader) that applies the log of commands accepted by the leader (with some lag). A tablet can have zero or more followers. Followers serve two primary purposes:

* In case of the leader's termination or failure, followers are the preferred [candidates](#tablet-candidate) to become the new leader because they can become the leader much faster than other candidates since they have applied most of the log.
* Followers can respond to read-only queries if a client explicitly chooses the optional relaxed transaction mode that allows for stale reads.

### Tablet generation {#tablet-generation}

A **tablet generation** is a number identifying the reincarnation of the tablet leader. It changes only when a new leader is chosen and always grows.

### Tablet local database {#local-database}

A **tablet local database** or **local database** is a set of data structures and related code that manages the tablet's state and the data it stores. Logically, the local database state is represented by a set of tables very similar to relational tables. Modification of the state of the local database is performed by local tablet transactions generated by the tablet's user actor.
=======
A **tablet follower** (also known as a **hot standby**) is a copy of the [tablet leader](#tablet-leader) that applies the log of commands accepted by the leader (with some delay). A tablet can have zero or more replicas. Replicas perform two main functions:

* In case the leader terminates or fails, replicas are preferred [candidates](#tablet-candidate) for the new leader, as they can become the leader much faster than other candidates because they have applied most of the log.
* Replicas can respond to read-only requests if the client explicitly chooses an optional relaxed transaction mode that allows stale reads.

### Tablet generation {#tablet-generation}

A **tablet generation** is a number that identifies the reincarnation of the tablet leader. It changes only when a new leader is elected and always increases.

### Tablet local database {#local-database}

**tablet local database** or **local database** — is a set of data structures and associated code that manage the state of a tablet and the data it stores. Logically, the state of the local database is represented by a set of tables, very similar to relational tables. Modification of the local database state is performed by local tablet transactions created by the tablet's user actor.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

Each local database table is stored using the [LSM tree](#lsm-tree) data structure.

#### Log-structured merge-tree {#lsm-tree}

<<<<<<< HEAD
A **[log-structured merge-tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree)** or **LSM tree**, is a data structure designed to optimize write and read performance in storage systems. It is used in {{ ydb-short-name }} for storing [local database](#local-database) tables and [VDisks](#vdisk) data.
=======
**[Log-structured merge-tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree)** is a data structure designed to optimize write and read performance in storage systems. It is used in {{ ydb-short-name }} to store tables of the [local database](#local-database) and data of [VDisks](#vdisk).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### MemTable {#memtable}

All data written to a [local database](#local-database) tables is initially stored in an in-memory data structure called a **MemTable**. When the MemTable reaches a predefined size, it is flushed to disk as an immutable [SST](#sst).

#### Sorted string table {#sst}

<<<<<<< HEAD
A **sorted string table** or **SST** is an immutable data structure that stores table rows sorted by key, facilitating efficient key lookups and range queries. Each SST is composed of a contiguous series of small data pages, typically around 7 KiB in size each, which further optimizes the process of reading data from disk. An SST typically represents a part of [LSM tree](#lsm-tree).

#### Tablet pipe {#tablet-pipe}

A **Tablet pipe** or **TabletPipe** is a virtual connection that can be established with a tablet. It includes resolving the [tablet leader](#tablet-leader) by [TabletID](#tabletid). It is the recommended way to work with the tablet. The term **open a pipe to a tablet** describes the process of resolving (searching) a tablet in a cluster and establishing a virtual communication channel with it.
=======
**Sorted string table** or **SST** is an immutable data structure that stores table rows sorted by key, facilitating efficient key lookup and range scans. Each SST consists of a continuous series of small data pages, typically about 7 KiB each, which further optimizes reading data from disk. An SST is usually part of an [LSM tree](#lsm-tree).

#### Tablet pipe {#tablet-pipe}

**tablet pipe** or **TabletPipe** is a virtual connection that can be established with a tablet. It includes finding the [tablet leader](#tablet-leader) by [TabletID](#tabletid). This is the recommended way to work with a tablet. The term **open a pipe to a tablet** describes the process of resolving (finding) a tablet in the cluster and establishing a virtual communication channel with it.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### TabletID {#tabletid}

A **TabletID** is a cluster-wide unique [tablet](#tablet) identifier.

#### Bootstrapper {#bootstrapper}

<<<<<<< HEAD
The **bootstrapper** is the primary mechanism for launching tablets, used for service tablets (for example, for [Hive](#hive), [DS controller](#ds-controller), root [SchemeShard](#scheme-shard)). The [Hive](#hive) tablet initializes the rest of the tablets.
=======
**Bootstrapper** is the main mechanism for starting tablets, used for system tablets (e.g., [Hive](#hive), [DS controller](#ds-controller), root [SchemeShard](#scheme-shard)). [Hive](#hive) initializes the remaining tablets.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

### Shared cache {#shared-cache}

A **shared cache** is an [actor](#actor) that stores data pages recently accessed and read from [distributed storage](#distributed-storage). Caching these pages reduces disk I/O operations and accelerates data retrieval, enhancing overall system performance.

### Memory controller {#memory-controller}

A **memory controller** is an [actor](#actor) that manages {{ ydb-short-name }} [memory limits](../reference/configuration/memory_controller_config.md).

### Spilling {#spilling}

**Spilling** is a memory management mechanism in {{ ydb-short-name }} that temporarily offloads intermediate query data to external storage when such data exceeds the available node RAM capacity. In {{ ydb-short-name }}, disk storage is currently used for spilling.

For more details on spilling, see [{#T}](query_execution/spilling.md).

### Tablet types {#tablet-types}

[Tablets](#tablet) can be considered a framework for building reliable components operating in a distributed system. {{ ydb-short-name }} has multiple components implemented using this framework, listed below.

#### Scheme shard {#scheme-shard}

A **Scheme shard** or **SchemeShard** is a tablet that stores a database schema, including metadata of user [tables](#table), [topics](#topic), etc.

Additionally, there is a **root scheme shard**, which stores information about databases created in a cluster.

#### Data shard {#data-shard}

A **data shard** or **DataShard** is a tablet that manages a segment of a [row-oriented user table](datamodel/table.md#row-oriented-tables). The logical user table is divided into segments by continuous ranges of the primary key of the table. Each such range is managed by a separate DataShard tablet instance. Such ranges are also called [partitions](#partition). DataShard tablets store data row by row, which is efficient for OLTP workloads.

#### Column shard {#column-shard}

A **column shard** or **ColumnShard** is a tablet that stores a data segment of a [column-oriented user table](datamodel/table.md#column-oriented-tables).

#### KV Tablet {#kv-tablet}

<<<<<<< HEAD
A **KV Tablet** or **key-value tablet** is a tablet that implements a simple key->value mapping, where keys and values are strings. It also has a number of specific features, like locks.
=======
**KeyValue**, **KV Tablet** is a tablet that implements a simple key → value mapping, where keys and values are strings. It also has several specific features, such as locks.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### PQ Tablet {#pq-tablet}

<<<<<<< HEAD
A **PQ Tablet** or **persistent queue tablet** is a tablet that implements the concept of a [topic](#topic). Each topic consists of one or more partitions, and each partition is managed by a separate PQ tablet instance.
=======
**PersQueue** or **persistent queue tablet** is a tablet that implements the concept of a [topic](#topic). Each topic consists of one or more partitions, and each partition is managed by a separate instance of the PQ tablet.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### TxAllocator {#txallocator}

A **TxAllocator** or **transaction allocator** is a system tablet that allocates unique transaction identifiers ([TxID](#txid)) within the cluster. Typically, a cluster has several such tablets, from which [transaction proxy](#transaction-proxy) pre-allocates and caches ranges for local issuance within a single process.

#### Coordinator {#coordinator}

<<<<<<< HEAD
The **Coordinator** is a system tablet that ensures the global ordering of transactions. The coordinator's task is to assign a logical [PlanStep](#planstep) time to each transaction planned through this coordinator. Each transaction is assigned exactly one coordinator, chosen by hashing its [TxId](#txid).
=======
**coordinator** is a system tablet that ensures global ordering of transactions. The coordinator's task is to assign a logical time [PlanStep](#planstep) to each transaction planned through this coordinator. Each transaction is assigned exactly one coordinator, selected by hashing its [TxId](#txid).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### Mediator {#mediator}

The **Mediator** is a system tablet that distributes the transactions planned by [coordinators](#coordinator) to the transaction participants (usually, [DataShards](#data-shard)). Mediators ensure the advancement of global time. Each transaction participant is associated with exactly one mediator. Mediators allow to avoid the need for a full mesh of connections between all coordinators and all participants in all transactions.

#### Hive {#hive}

<<<<<<< HEAD
A **Hive** is a system tablet responsible for launching and managing other tablets. Its responsibilities include moving tablets between nodes in case of [node](#node) failure or overload.{% if audience != "corp" %} You can learn more about Hive in a [dedicated article](../contributor/hive.md).{% endif %}
=======
**Hive** is a system tablet responsible for starting and managing other tablets. Its responsibilities include moving tablets between nodes in case of failure or overload of a [node](#node).{% if audience != "corp" %} More information about Hive can be found in a [separate article](../contributor/hive.md).{% endif %}
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### Cluster management system {#cms}

The **cluster management system** or **CMS** is a system tablet responsible for managing the information about the current [{{ ydb-short-name }} cluster](#cluster) state. This information is used to perform cluster rolling restarts without affecting user workloads, maintenance, cluster re-configuration, etc.

#### Node Broker {#node-broker}

The **Node Broker** is a system tablet that registers [dynamic nodes](#dynamic-node) in the cluster.

#### BSController {#ds-controller}

**BSController**, **blob storage controller**, or **distributed storage controller** manages the dynamic configuration of distributed storage, including information about [PDisks](#pdisk), [VDisks](#vdisk), and [storage groups](#storage-group). It interacts with [node warden](#node-warden) to launch various distributed storage components. It interacts with [Hive](#hive) to allocate [channels](#channel) to tablets.

#### Console {#console}

**Console** is a system tablet responsible for storing [dynamic configuration](../devops/configuration-management/configuration-v1/dynamic-config.md) and delivering it to cluster nodes.

#### Kesus {#kesus}

**Kesus** is a tablet that implements a [coordination node](datamodel/coordination-node.md).

#### SysViewProcessor {#sys-view-processor}

<<<<<<< HEAD
**SysViewProcessor** is a tablet that stores data for some of the [system views](../dev/system-views.md).
=======
**SysViewProcessor** is a tablet that stores data of some [system views](../dev/system-views.md).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

{% if feature_serial %}

#### SequenceShard {#sequence-shard}

**SequenceShard** is a tablet that serves Sequence objects used to implement [serial data types](../yql/reference/types/serial.md).

{% endif %}

{% if feature_async_replication %}

#### ReplicationController {#replication-controller}

**ReplicationController** is a tablet responsible for the [asynchronous replication](async-replication.md) process.

{% endif %}

#### StatisticsAggregator {#statistics-aggregator}

**StatisticsAggregator** is a tablet responsible for collecting statistics used in cost-based optimization.

### Slot {#slot}

A **slot** in {{ ydb-short-name }} can be used in two contexts:

<<<<<<< HEAD
* **Slot** is a portion of a server's resources allocated to running a single {{ ydb-short-name }} [node](#node). A common slot size is 10 CPU cores and 50 GB of RAM. Slots are used if a {{ ydb-short-name }} cluster is deployed on servers or virtual machines with sufficient resources to host multiple slots.
* [VDisk](#slot) **slot** or **VSlot** is a fraction of [PDisk](#pdisk) that can be allocated to one of the [VDisks](#vdisk).
=======
* **Slot** is a portion of server resources allocated to run one [node](#node) {{ ydb-short-name }}. The typical slot size is 10 CPU cores and 50 GB of RAM. Slots are used when the cluster {{ ydb-short-name }} is deployed on servers or virtual machines with sufficient resources to host multiple slots.
* **VDisk slot** or **VSlot** is a share of [PDisk](#pdisk) that can be allocated to one of the [VDisk](#vdisk).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

### State storage {#state-storage}

A **State storage** or **StateStorage** is a distributed service that stores information about tablets, namely:

* The current leader of the tablet or its absence.
* Tablet followers.
* Generation and step of the tablet `(generation:step)`.

State storage is used as a name service for resolving tablets, i.e., getting [ActorId](#actorid) by [TabletID](#tabletid). StateStorage is also used in the process of electing the [tablet leader](#tablet-leader).

<<<<<<< HEAD
Information in state storage is volatile. Thus, it is lost when the power is turned off, or the process is restarted. Despite the name, this service is not persistent storage. It contains only information that is easily recoverable and does not have to be durable. However, state storage keeps information on several nodes to minimize the impact of node failures. Through this service, it is possible to gather a quorum, which is used to elect tablet leaders.

Due to its nature, the state storage service operates in a best-effort manner. For example, the absence of several tablet leaders is guaranteed through the leader election protocol on [distributed storage](#distributed-storage), not state storage.
=======
Information in the state storage is volatile. Thus, it is lost on power loss or process restart. Despite its name, this service is not a permanent long-term storage. It only contains information that is easy to recover and that does not need to be durable. However, state storage stores information on multiple nodes to minimize the impact of node failures. This service can also be used to gather a quorum, which is used for tablet leader election.

Due to its nature, the state storage service operates on a best-effort basis. For example, the absence of multiple tablet leaders is guaranteed through the leader election protocol on [distributed storage](#distributed-storage), not on state storage.

For more details on the StateStorage architecture and related subsystems, see the Metadata distribution services section.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

### Board {#board}

**Board** is a distributed service for storing metadata as key-value pairs. It is used, among other things, to store information about [endpoints](connect.md#endpoint).

<<<<<<< HEAD
### Scheme board {#scheme-board}

**SchemeBoard** is a distributed service for storing metadata as key-value pairs. It is used, among other things, to store information about [schemes](#global-schema).
=======
For more details on the Board architecture and related subsystems, see the Metadata distribution services section.

### SchemeBoard {#scheme-board}

**SchemeBoard** is a distributed service designed to store metadata as key-value pairs. It is used, among other things, to store information about [schemas](#global-schema).

For more details on the SchemeBoard architecture and related subsystems, see the Metadata distribution services section.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### Compaction {#compaction}

**Compaction** is the internal background process of rebuilding [LSM tree](#lsm-tree) data. The data in [VDisks](#vdisk) and [local databases](#local-database) are organized in the form of an LSM tree. Therefore, there is a distinction between **VDisk compaction** and **Tablet compaction**. The compaction process is usually quite resource-intensive, so efforts are made to minimize the overhead associated with it, for example, by limiting the number of concurrent compactions.

#### gRPC proxy {#grpc-proxy}

<<<<<<< HEAD
A **gRPC Proxy** is the client proxy system for external user requests. Client requests enter the system via the [gRPC](https://grpc.io) protocol, then the proxy component translates them into internal calls for executing these requests, passed around via [Interconnect](#actor-system-interconnect). This proxy provides an interface for both request-response and bidirectional streaming.

### Distributed configuration {#distributed-configuration}

**Distributed configuration** or **DistConf** is an internal cluster [configuration](../devops/configuration-management/configuration-v2/config-overview.md) mechanism that handles startup and configuration of [static nodes](#static-node), automatic management of [static storage groups](#static-group), and [State storage](#state-storage). Distributed configuration starts before any [tablets](#tablet), [storage groups](#storage-group), or [State storage](#state-storage).

For more on how distributed configuration works, see [{#T}](../contributor/configuration-v2.md).

### Distributed storage implementation {#distributed-storage-implementation}

**Distributed storage** is a distributed fault-tolerant data storage layer that persists binary records called [LogoBlob](#logoblob), addressed by a particular type of identifier called [LogoBlobID](#logoblobid). Thus, distributed storage is a key-value store that maps LogoBlobID to a string up to 10MB in size. Distributed storage consists of many [storage groups](#storage-group), each being an independent data repository.
=======
**gRPC proxy** is a proxy system for external user requests. Client requests enter the system via the [gRPC](https://grpc.io) protocol, then the proxy component translates them into internal calls to execute these requests, transmitted over [interconnect](#actor-system-interconnect). This proxy provides an interface for both request-response and bidirectional streaming.

### Distributed configuration {#distributed-configuration}

**Distributed configuration** or **DistConf** is an internal [configuration](../devops/configuration-management/configuration-v2/config-overview.md) mechanism of the cluster that provides startup and configuration of [static nodes](#static-node), automatic management of the [static storage group](#static-group) and [State Storage](../concepts/glossary.md#state-storage). Distributed configuration starts before any [tablets](#tablet), [storage groups](#storage-group), and [State Storage](../concepts/glossary.md#state-storage).

For more details on the distributed configuration architecture, see [{#T}](../contributor/configuration-v2.md).

### Distributed storage implementation {#distributed-storage-implementation}

**Distributed storage** is a distributed fault-tolerant data storage layer that stores binary records called [LogoBlob](#logoblob), addressed using a specific type of identifier called [LogoBlobID](#logoblobid). Thus, distributed storage is a key-value store that maps a LogoBlobID to a string of up to 10 MB. Distributed storage consists of multiple [storage groups](#storage-group), each of which is an independent data repository.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

Distributed storage persists immutable data, with each immutable blob identified by a specific `LogoBlobID` key. The distributed storage API is very specific, designed only for use by [tablets](#tablet) to store their data and log changes, not for general-purpose data storage. Data in distributed storage is deleted using special barrier commands. Due to the lack of mutations in its interface, distributed storage can be implemented without implementing [distributed consensus](https://en.wikipedia.org/wiki/Consensus_(computer_science)). Moreover, distributed storage is just a building block tablets use to implement distributed consensus.

#### LogoBlob {#logoblob}

<<<<<<< HEAD
A **LogoBlob** is a piece of binary immutable data identified by [LogoBlobID](#logoblobid) and stored in [Distributed storage](#distributed-storage). The blob size is limited at the [VDisk](#vdisk) level and higher on the stack. Currently, the maximum blob size VDisks are ready to process is 10 MB.
=======
**LogoBlob** is a set of binary immutable data identified by a [LogoBlobID](#logoblobid) and stored in [distributed storage](#distributed-storage). The data block size is limited at the [VDisk](#vdisk) level and above in the stack. Currently, the maximum data block size that a VDisk can handle is 10 MB.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### LogoBlobID {#logoblobid}

A **LogoBlobID** is the [LogoBlob](#logoblob) identifier in the [Distributed storage](#distributed-storage). It has a structure of the form `[TabletID, Generation, Step, Channel, Cookie, BlobSize, PartID]`. The key elements of LogoBlobID are:

* `TabletID` is an [ID](#tabletid) of the tablet that the LogoBlob belongs to.
* `Generation` is the generation of the tablet in which the blob was recorded.
* `Channel` is the tablet [channel](#channel) where the LogoBlob is recorded.
* `Step` is an incremental counter, usually within the tablet generation.
<<<<<<< HEAD
* `Cookie` is a unique blob identifier within a single `Step`. A cookie is usually used when writing several blobs within a single `Step`.
* `BlobSize` is the LogoBlob size.
* `PartID` is the identifier of the blob part. It is crucial when the original LogoBlob is broken into parts using [erasure coding](#erasure-coding), and the parts are written to the corresponding [VDisks](#vdisk) and [storage groups](#storage-group).

#### Replication {#replication}

**Replication** is a process that ensures there are always enough copies (replicas) of data to maintain the desired availability characteristics of a {{ ydb-short-name }} cluster. Typically, it is used in geo-distributed {{ ydb-short-name }} clusters.
=======
* `Cookie` is a unique identifier of a data block within a single `Step`. The cookie is typically used when writing multiple data blocks to one `Step`.
* `BlobSize` is the size of the LogoBlob.
* `PartID` is the identifier of a data block part. It is important when the original LogoBlob is split into parts using [erasure coding](#erasure-coding), and the parts are written to the corresponding [VDisk](#vdisk) and [storage groups](#storage-group).

#### Replication {#replication}

**Replication** is a process that ensures a sufficient number of copies (replicas) of data are available to maintain the desired availability characteristics of a {{ ydb-short-name }} cluster. It is typically used in geo-distributed {{ ydb-short-name }} clusters.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### Erasure Coding {#erasure-coding}

<<<<<<< HEAD
[**Erasure coding**](https://en.wikipedia.org/wiki/Erasure_code) is a method of data encoding in which the original data is supplemented with redundancy and divided into several fragments, providing the ability to restore the original data if one or more fragments are lost. It is widely used in [single-AZ](#regions-az) {{ ydb-short-name }} clusters as opposed to [replication](#replication) with 3 replicas. For example, the most popular 4+2 scheme provides the same reliability as three replicas, with space redundancy of 1.5 versus 3.

#### PDisk {#pdisk}

**PDisk** or **Physical disk** is a component that controls a physical disk drive (block device). In other words, PDisk is a subsystem that implements an abstraction similar to a specialized file system on top of block devices (or files simulating a block device for testing purposes). PDisk provides data integrity controls (including [erasure encoding](#erasure-coding) of sector groups for data recovery on single bad sectors, integrity control with checksums), transparent data-at-rest encryption of all disk data, and transactional guarantees of disk operations (write confirmation strictly after `fsync`).
=======
[**Erasure coding**](https://en.wikipedia.org/wiki/Erasure_code) is a data encoding method where the original data is supplemented with redundancy and split into multiple fragments, enabling recovery of the original data if one or more fragments are lost. It is widely used in {{ ydb-short-name }} clusters with a single [availability zone](#regions-az), as opposed to [replication](#replication) with 3 replicas. For example, the most popular erasure coding scheme 4+2 provides the same reliability as three replicas, with a space overhead of 1.5 compared to 3.

#### PDisk {#pdisk}

**PDisk** or **physical disk** is a component that controls a physical disk drive (block device). In other words, PDisk is a subsystem that implements an abstraction similar to a specialized file system on top of block devices (or files emulating a block device for testing purposes). PDisk provides data integrity control (including [erasure coding](#erasure-coding) of sector groups to recover data on individual damaged sectors, integrity control using checksums), transparent encryption of all data on the disk, and transactional guarantees for disk operations (write confirmation strictly after `fsync`).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

PDisk contains a scheduler that provides device bandwidth sharing between several clients ([VDisks](#vdisk)). PDisk divides a block device into chunks called [slots](#slot) (about 128 megabytes in size; smaller chunks are allowed). No more than 1 VDisk can own each slot at a time. PDisk also supports a recovery log shared between PDisk service records and all VDisks.

#### VDisk {#vdisk}

<<<<<<< HEAD
**VDisk** or **Virtual disk** is a component that implements the persistence of [distributed storage](#distributed-storage) [LogoBlobs](#logoblob) on [PDisks](#pdisk). VDisk stores all its data on PDisks. One VDisk corresponds to one PDisk, but usually, several VDisks are linked to one PDisk. Unlike PDisk, which hides chunks and logs behind it, VDisk provides an interface at the LogoBlob and [LogoBlobID](#logoblobid) level, like writing LogoBlob, reading LogoBlobID data, and deleting a set of LogoBlob using a special command. VDisk is a member of a [storage group](#storage-group). VDisk itself is local, but many VDisks in a given group provide reliable data storage. The VDisks in a group synchronize the data with each other and replicate the data in case of loss. A set of VDisks in a storage group forms a distributed RAID.
=======
**VDisk** or **virtual disk** is a component that implements data storage of [distributed storage](#distributed-storage) [LogoBlob](#logoblob) on [PDisk](#pdisk). VDisk stores all its data on PDisk. One VDisk corresponds to one PDisk, but typically several VDisks are associated with one PDisk. Unlike PDisk, which hides blocks and logs behind it, VDisk provides an interface at the LogoBlob and [LogoBlobID](#logoblobid) level, for example writing a LogoBlob, reading LogoBlobID data, and deleting a set of LogoBlobs using a special command. VDisk is a member of a [storage group](#storage-group). VDisk itself is local, but many VDisks in a given group provide reliable data storage. VDisks in a group synchronize data with each other and replicate data in case of losses. The set of VDisks in a storage group forms a distributed RAID.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### Yard {#yard}

**Yard** is the name of the [PDisk](#pdisk) API. It allows [VDisk](#vdisk) to read and write data to chunks and logs, reserve chunks, delete chunks, and transactionally receive and return ownership of chunks. In some contexts, Yard can be considered to be a synonym for PDisk.

#### Skeleton {#skeleton}

A **Skeleton** is an [actor](#actor) that provides an interface to a [VDisk](#vdisk).

#### SkeletonFront {#skeletonfront}

**SkeletonFront** is a proxy actor for Skeleton that controls the flow of messages coming to Skeleton.

#### Distributed storage controller {#ds-controller}

The **distributed storage controller** or **DS controller** manages the dynamic configuration of distributed storage, including information about [PDisks](#pdisk), [VDisks](#vdisk), and [storage groups](#storage-group). It interacts with [node wardens](#node-warden) to launch various distributed storage components. It interacts with [Hive](#hive) to allocate [channels](#channel) to [tablets](#tablet).

#### Proxy {#ds-proxy}

The **distributed storage proxy**, **DS proxy**, or **BS proxy** plays the role of a client library for performing operations with [Distributed storage](#distributed-storage). DS Proxy users are [tablets](#tablet) that write to and read from Distributed storage. DS Proxy hides the distributed nature of Distributed storage from the user. The task of DS Proxy is to write to the quorum of the [VDisks](#vdisk), make retries if necessary, and control the write/read flow to avoid overloading VDisks.

<<<<<<< HEAD
Technically, DS Proxy is implemented as an [actor service](#actor-service) launched by the [node warden](#node-warden) on each node for each storage group, processing all requests to the group (writing, reading, and deleting [LogoBlobs](#logoblob), blocking the group). When writing data, DS proxy performs [erasure encoding](#erasure-coding) of data by dividing LogoBlobs into parts, which are then sent to the corresponding VDisks. DS Proxy performs the reverse process when reading, receiving parts from VDisks, and restoring LogoBlobs from them.
=======
Technically, DS-proxy is implemented as an [actor service](#actor-service) launched by [node warden](#node-warden) on each node for each storage group, handling all requests to the group (write, read, and delete of [LogoBlob](#logoblob), group locking). When writing data, DS-proxy performs [error-correcting coding](#erasure-coding) of the data, splitting the LogoBlob into parts that are then sent to the corresponding VDisks. DS-proxy performs the reverse process when reading, receiving parts from VDisks and reconstructing the LogoBlob from them.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### Node warden {#node-warden}

**Node warden** or `BS_NODE` is an [actor service](#actor-service) on each node of the cluster, launching [PDisks](#pdisk), [VDisks](#vdisk), and [DS proxies](#ds-proxy) of [static storage groups](#static-group) at the node start. Also, it interacts with the [DS controller](#ds-controller) to launch PDisk, VDisk, and DS proxies of [dynamic groups](#dynamic-group). The DS proxy of dynamic groups is launched on request: node warden processes "undelivered" messages to the DS proxy, launching the corresponding DS proxies and receiving the group configuration from the DS controller.

#### Fail realm {#fail-realm}

A **fail realm** is a set of [fail domains](#fail-domain) that are likely to fail simultaneously. The correlated failure of two [VDisks](#vdisk) within the same fail realm is more probable than that of two VDisks from different fail realms.

An example of a fail realm is a set of hardware located in the same [data center or availability zone](#regions-az) that can all fail together due to a natural disaster, major power outage, or similar event.

#### Failure domain {#fail-domain}

<<<<<<< HEAD
A **fail domain** is a set of hardware that may fail simultaneously. The correlated failure of two [VDisks](#vdisk) within the same fail domain is more probable than the failure of two VDisks from different fail domains. In the case of different fail domains, this probability is also affected by whether these domains belong to the same [fail realm](#fail-realm) or not.

For example, a fail domain includes disks on the same server, as all server disks may become unavailable if the server's PSU or network controller fails. A fail domain also typically includes servers located in the same server rack, as all the hardware in the rack may become unavailable if there is a power outage or an issue with the network hardware in the same rack. Thus, the typical fail domain corresponds to a server rack if the [cluster](#cluster) is configured to be rack-aware, or to a server otherwise.
=======
A **fail domain** is a set of equipment that can fail simultaneously. A correlated failure of two [VDisks](#vdisk) in the same fail domain is more likely than a failure of two VDisks from different fail domains. In the case of different fail domains, the probability of simultaneous failure also depends on whether the domains in question belong to the same fail realm or different ones.

An example of a fail domain is a set of disks connected to a single server, since all disks of a particular server may become unavailable if the server's power supply or network controller fails. Typically, all servers located in a single [server rack](#rack) are considered to belong to a common fail domain, because power or network issues at the rack level cause all equipment in it to become unavailable. Thus, a typical fail domain corresponds to a server rack (if the [cluster](#cluster) is configured with rack-aware topology) or a single server.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

Domain failures are handled automatically by {{ ydb-short-name }} without shutting down the cluster.

#### Distributed storage channel {#channel}

<<<<<<< HEAD
A **channel** is a logical connection between a [tablet](#tablet) and [Distributed storage](#distributed-storage) group. The tablet can write data to different channels, and each channel is mapped to a specific [storage group](#storage-group). Having multiple channels allows the tablet to:
=======
A **distributed storage channel**, **DS channel**, or **channel** is a logical connection between a [tablet](#tablet) and a [distributed storage](#distributed-storage) group. A tablet can write data to different channels, and each channel maps to a specific [storage group](#storage-group). Having multiple channels allows a tablet to:
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

* Record more data than one storage group can contain.
* Store different [LogoBlobs](#logoblob) on different storage groups, with different properties like erasure encoding or on different storage media (HDD, SSD, NVMe).

### Distributed transactions implementation {#transaction-implementation}

<<<<<<< HEAD
Terms related to the implementation of [distributed transactions](#transactions) are explained below.{% if oss == true %} The implementation itself is described in a separate article [{#T}](../contributor/datashard-distributed-txs.md).{% endif %}

#### Deterministic transactions {#deterministic-transactions}

{{ ydb-short-name }} distributed transactions are inspired by the research paper [Building Deterministic Transaction Processing Systems without Deterministic Thread Scheduling](http://cs-www.cs.yale.edu/homes/dna/papers/transactions-wodet11.pdf) by Alexander Thomson and Daniel J. Abadi from Yale University. The paper introduced the concept of **deterministic transaction** processing, which allows for highly efficient distributed processing of transactions. The original paper imposed limitations on what kinds of operations can be executed in this manner. As these limitations interfered with real-world user scenarios, {{ ydb-short-name }} evolved its algorithms to overcome them by using deterministic transactions as stages of executing user transactions with additional orchestration and locking.

#### Optimistic locking {#optimistic-locking}

As in many other database management systems, {{ ydb-short-name }} queries can put locks on certain pieces of data, like table rows, to ensure that concurrent access does not modify them into an inconsistent state. However, {{ ydb-short-name }} checks these locks not at the beginning of transactions but during commit attempts. The former is called **pessimistic locking** (used in PostgreSQL, for example), while the latter is called **optimistic locking** (used in {{ ydb-short-name }}).

#### Transaction lock invalidation {#tli}

**Transaction lock invalidation** (TLI) is the standard behavior of {{ ydb-short-name }} when parallel transactions conflict under [optimistic locking](#optimistic-locking). If one transaction (the breaker) writes data and thereby breaks the locks of another transaction (the victim), {{ ydb-short-name }} detects this at the victim's commit time and rolls it back with a `transaction locks invalidated` error. For more information about TLI diagnostics, see [{#T}](../troubleshooting/performance/queries/transaction-lock-invalidation.md).
=======
Below are explained terms related to the implementation of [distributed transactions](#transactions).{% if oss == true %} The implementation itself is described in a separate article [{#T}](../contributor/datashard-distributed-txs.md).{% endif %}

#### Deterministic transactions {#deterministic-transactions}

Distributed transactions in {{ ydb-short-name }} are inspired by the research paper [Building Deterministic Transaction Processing Systems without Deterministic Thread Scheduling](http://cs-www.cs.yale.edu/homes/dna/papers/transactions-wodet11.pdf) by Alexander Thomson and Daniel J. Abadi from Yale University. The paper introduced the concept of **deterministic transaction processing**, which allows efficient processing of distributed transactions. The original paper imposed restrictions on the types of operations that could be performed in this way. Since these restrictions hindered real user scenarios, {{ ydb-short-name }} evolved its algorithms to handle these restrictions, using deterministic transactions as stages of user transaction execution with additional orchestration and locking.

#### Optimistic locking {#optimistic-locking}

As in many other database management systems, queries in {{ ydb-short-name }} can place locks on certain data fragments, such as table rows, to ensure that concurrent changes do not lead to an inconsistent state. However, {{ ydb-short-name }} checks these locks not at the beginning of transactions, but when attempting to commit them. The first approach is called **pessimistic locking** (for example, used in PostgreSQL), and the second is called **optimistic locking** (used in {{ ydb-short-name }}).

#### Transaction lock invalidation {#tli}

**Transaction Lock Invalidation** (**TLI**) is normal behavior {{ ydb-short-name }} when parallel transactions conflict within [optimistic locks](#optimistic-locking). If one transaction (the violator) writes data and thereby breaks the locks of another transaction (the victim), {{ ydb-short-name }} detects this when the victim commits and rolls it back with error `transaction locks invalidated`. For more details on TLI diagnostics, see [{#T}](../troubleshooting/performance/queries/transaction-lock-invalidation.md).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### Prepare stage {#prepare-stage}

<<<<<<< HEAD
The **prepare stage** is a phase of distributed transaction execution, during which the transaction body is registered on all participating shards.
=======
**Preparation phase** is a transaction phase during which the transaction body is registered on all participating shards.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### Execute stage {#execute-stage}

<<<<<<< HEAD
The **execute stage** is a phase of distributed query execution in which the scheduled transaction is executed and the response is generated.

In some cases, instead of [prepare](#prepare-stage) and execute, the transaction is immediately executed, and a response is generated.  For example, this happens for transactions involving only one shard or consistent reads from a snapshot.

#### Dirty operations {#dirty-operations}

In the case of read-only transactions, similar to "read uncommitted" in other database management systems, it might be necessary to read data that has not yet been committed to disk. This is called **dirty operations**.

#### Read-write set {#rw-set}

The **read-write set** or **RW set** is a set of data that will participate in executing a [distributed transaction](#transactions). It combines the read set, the data that will be read, and the write set, the data modifications to be carried out.

#### Read set {#read-set}

The **read set** or **ReadSet data** is what participating shards forward during the transaction execution. In the case of data transactions, it may contain information about the state of [optimistic locks](#optimistic-locking), the readiness of the shard for commit, or the decision to cancel the transaction.
=======
**Execution phase** is a transaction phase during which the scheduled transaction is executed and a response is generated.

In some cases, instead of [preparation](#prepare-stage) and execution, the transaction is executed immediately and a response is generated. For example, this happens for transactions affecting only one shard or for consistent reads from a data snapshot (snapshot).

#### Dirty operations {#dirty-operations}

In the case of read-only transactions, similar to "read uncommitted" in other database management systems, it may be necessary to read data that has not yet been committed to disk. This is called **dirty operations**.

#### Read-write set {#rw-set}

**Read-write set** or **RW-set** is a set of data that will participate in the execution of a [distributed transaction](#transactions). It combines the data of the read set, which will be read, and the write set, for which modifications will be performed.

#### Read set {#read-set}

**Read set** or **ReadSet data** is what participating shards send during transaction execution. In the case of data transactions, it may contain information about the state of [optimistic locks](#optimistic-locking), the shard's readiness to commit, or a decision to abort the transaction.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### Transaction proxy {#transaction-proxy}

<<<<<<< HEAD
The **transaction proxy** or `TX_PROXY` is a service that orchestrates the execution of many [distributed transactions](#transactions): sequential phases, phase execution, planning, and aggregation of results. In the case of direct orchestration by other actors (for example, QP data transactions), it is used for caching and allocation of unique [TxIDs](#txid).

#### Transaction flags {#txflags}

**Transaction flags** or **TxFlags** is a bitmask of flags that modify the execution of a transaction in some way.

#### Transaction ID {#txid}

**Transaction ID** or **TxID** is a unique identifier assigned to each transaction when it is accepted by {{ ydb-short-name }}.

#### Transaction order ID {#transaction-order-id}

A **transaction order ID** is a unique identifier assigned to each transaction during planning. It consists of [PlanStep](#planstep) and [Transaction ID](#txid).
=======
**Transaction proxy** or `TX_PROXY` is a service that orchestrates the execution of many [distributed transactions](#transactions): sequential phases, phase execution, scheduling, and result aggregation. In the case of direct orchestration by other actors (for example, QP data transactions), it is used for caching and allocating unique [TxIDs](#txid).

#### Transaction flags {#txflags}

**Transaction flags** or **TxFlags** is a bitmask of flags that somehow modify the execution of a transaction.

#### Transaction ID {#txid}

**Transaction ID** or **TxID** is a unique identifier assigned to each transaction when it is accepted {{ ydb-short-name }}.

#### Transaction order ID {#transaction-order-id}

**Transaction order id** is a unique identifier assigned to each transaction during scheduling. It consists of [PlanStep](#planstep) and [Transaction ID](#txid).
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### PlanStep {#planstep}

<<<<<<< HEAD
**PlanStep** or **step** is the logical time for which a set of transactions is planned to be executed.

#### Mediator time {#mediator-time}

During the distributed query execution, **mediator time** is the logical time before which (inclusive) the shard participant must know the entire execution plan. It is used to advance the time in the absence of transactions on a particular shard, to determine whether it can read from a snapshot.

#### MiniKQL {#minikql}

**MiniKQL** is a language that allows the expression of a single [deterministic transaction](#deterministic-transactions) in the system. It is a functional, strongly typed language. Conceptually, the language describes a graph of reading from the database, performing calculations on the read data, and writing the results to the database and/or to a special document representing the query result (shown to the user). The MiniKQL transaction must explicitly set its read set (readable data) and assume a deterministic selection of execution branches (for example, there is no random).

MiniKQL is a low-level language. The system's end users only see queries in the [YQL](#yql) language, which relies on MiniKQL in its implementation.
=======
**PlanStep** or **Step** is the logical time at which the execution of a set of transactions is scheduled.

#### Mediator time {#mediator-time}

During the execution of distributed transactions, **mediator time** is the logical time up to which (inclusive) a participating shard must know the entire execution plan. It is used to advance time when there are no transactions on a particular shard, to determine whether it can read from a snapshot.

#### MiniKQL {#minikql}

**MiniKQL** is a language that allows expressing a single [deterministic transaction](#deterministic-transactions) in the system. It is a functional, strongly typed language. Conceptually, the language describes a graph of reading from the database, performing computations on the read data, and writing results to the database and/or to a special document representing the query result (for display to the user). A MiniKQL transaction must explicitly specify its read set (data to be read) and assume deterministic branching (for example, no randomness).

MiniKQL is a low-level language. End users of the system only see queries in [YQL](#yql), which relies on MiniKQL in its implementation.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))

#### Query Processor {#kqp}

**QP** or **Query Processor** (previously, **KQP**) is a {{ ydb-short-name }} component responsible for the orchestration of user query execution and generating the final response.

### Global schema {#global-schema}

<<<<<<< HEAD
**Global Schema**, **Global Scheme**, or **Database Schema** is a schema of all the data stored in a [database](#database). It consists of [tables](#table) and other entities, such as [topics](#topic). The metadata about these entities is called a global schema. The term is used in contrast to **Local Schema**, which refers to the data schema inside a [tablet](#tablet). {{ ydb-short-name }} users never see the local schema and only work with the global schema.

### KiKiMR {#kikimr}

**KiKiMR** is the legacy name of {{ ydb-short-name }} that was used long before it became an [open-source product](https://github.com/ydb-platform/ydb). It can still be occasionally found in the source code, old articles and videos, etc.
=======
**Global scheme**, **global schema**, or **database schema** is the schema of all data stored in a [database](#database). It consists of [tables](#table) and other entities, such as [topics](#topic). Metadata about these entities is called the global schema. The term is used in contrast to **local schema**, which refers to the data schema inside a [tablet](#tablet). {{ ydb-short-name }} users never see the local schema and work only with the global schema.

### KiKiMR {#kikimr}

**KiKiMR** is the former name of {{ ydb-short-name }}, used before it became an [open-source product](https://github.com/ydb-platform/ydb). It may still be encountered in source code, old articles, videos, etc.
>>>>>>> 6c2f08c6922 (Auto-translate docs from PR #30237 (#48223))
