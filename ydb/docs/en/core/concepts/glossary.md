# Glossary {{ ydb-short-name }}

This article provides an overview of terms and definitions used in {{ ydb-short-name }} and its documentation. It [starts with key terms](#key-terminology) that are useful to know early in your work with {{ ydb-short-name }}, and the rest of the article contains [more advanced terms](#advanced-terminology) that may be useful later.

## Key terminology {#key-terminology}

This section describes terms that are useful for anyone working with {{ ydb-short-name }}, regardless of their role or use case.

### Cluster {#cluster}

**Cluster** (or **cluster** {{ ydb-short-name }}) is a set of interconnected [nodes](#node) {{ ydb-short-name }} that exchange data to process user queries and reliably store data. These nodes form one of the supported [cluster topologies](#topology), which directly affects the cluster's reliability and performance characteristics.

Clusters {{ ydb-short-name }} are multitenant and can contain multiple isolated [databases](#database).

### Database {#database}

As in most database management systems, a **database** in {{ ydb-short-name }} is a logical container for other entities, such as [tables](#table). However, in {{ ydb-short-name }}, the namespace within databases is hierarchical, similar to [virtual file systems](https://en.wikipedia.org/wiki/Virtual_file_system), and [directories](#folder) allow for more structured organization of entities.

Another important characteristic of {{ ydb-short-name }} databases is that they typically have dedicated computational resources. As a result, creating a database requires additional actions from [DevOps engineers](../devops/index.md).

### Node {#node}

{{ ydb-short-name }} **node** (or **node**) is a server process running an executable called `ydbd`. Multiple nodes {{ ydb-short-name }} can run on a single physical server or virtual machine, which is common practice. Thus, in the context of {{ ydb-short-name }}, nodes are **not** synonymous with hosts.

Since {{ ydb-short-name }} uses a storage and compute separation approach, `ydbd` has several operating modes that define the type of node. The available node types are described below.

#### Database node {#database-node}

**Database nodes** (also known as **tenant nodes**, **compute nodes**, **database nodes**, **tenant nodes**, or **compute nodes**) process user queries addressed to a specific logical [database](#database). Their state is stored only in RAM and can be restored from the [distributed storage](#distributed-storage). The collection of database nodes in a given [cluster {{ ydb-short-name }}](topology.md) can be considered the compute layer of that cluster. Thus, adding database nodes and allocating additional resources (CPU and RAM) to them are the main ways to increase the database's compute resources.

The primary role of database nodes is to run various [tablets](#tablet) and [actors](#actor) and to receive incoming network requests.

#### Storage node {#storage-node}

**Storage nodes** (or **storage nodes**) are stateful nodes responsible for long-term storage of data fragments. The collection of storage nodes in a given [cluster {{ ydb-short-name }}](#cluster) is called [distributed storage](#distributed-storage) and can be viewed as the storage layer of that cluster. Thus, adding more storage nodes and disks is the main way to increase the cluster's storage capacity and I/O throughput.

#### Hybrid node {#hybrid-mode}

A **hybrid node** is a process that simultaneously performs the roles of a [database node](#database-node) and a [storage node](#storage-node). Hybrid nodes are often used for development purposes. For example, you can run a container with a fully functional {{ ydb-short-name }} containing only one `ydbd` process in hybrid mode. They are rarely used in production environments.

#### Static node {#static-node}

**Static nodes** (or **static nodes**) are configured manually during initial cluster initialization or reconfiguration. Typically, they act as [storage nodes](#storage-node), but it is technically possible to configure them as [database nodes](#database-node) as well.

#### Dynamic node {#dynamic}

**Dynamic nodes** are added to and removed from the cluster on the fly. They can only serve as [database nodes](#database-node).

### Distributed storage {#distributed-storage}

**Distributed storage**, also known as **Blob storage** or **BlobStorage**, is a distributed fault-tolerant data storage layer in {{ ydb-short-name }}. It has a specialized API designed for storing immutable data fragments of a [tablet](#tablet).

Many terms related to the [implementation of distributed storage](#distributed-storage-implementation) are discussed below.

### Storage group {#storage-group}

A **storage group**, also called a **distributed storage group** or **Blob storage group**, is a place for reliable data storage, similar to [RAID](https://en.wikipedia.org/wiki/RAID) but using disks from multiple servers. Depending on the chosen [cluster topology](#topology), storage groups use different algorithms to ensure high availability, similar to [standard RAID levels](https://en.wikipedia.org/wiki/Standard_RAID_levels).

[Distributed storage](#distributed-storage) typically manages a large number of relatively small storage groups. Each group can be assigned to a specific [database](#database) to increase the disk space capacity and I/O throughput available to that database.

[Static](#static-group) and [dynamic](#dynamic-group) storage groups are physical, meaning their data is placed directly on [VDisk](#vdisk)s.

#### Static group {#static-group}

A **static group** is a special [storage group](#storage-group) created during the initial cluster deployment. Its main role is to store data of system [tablets](#tablet), which can be considered as cluster-wide metadata.

A static group may require special attention during major cluster maintenance, such as decommissioning an [availability zone](#regions-az).

#### Dynamic group {#dynamic-group}

Ordinary storage groups that are not [static](#static-group) are called **dynamic groups**. They are called dynamic because they can be created and deleted on the fly while the [cluster](#cluster) is running.

#### Virtual storage group {#virtual-storage-groups}

A **virtual storage group** is an entity that is not actually a [storage group](#storage-group) but appears as one from the outside (provides a similar external interface). It can store its data in other storage groups or in S3.

### Storage pool {#storage-pool}

A **storage pool** is a set of data storage devices with similar characteristics. Each storage pool is assigned a unique name within the {{ ydb-short-name }} cluster. Technically, each storage pool consists of multiple physical disks ( [PDisk](#pdisk)). Each [storage group](#storage-group) is created in a specific storage pool, which determines the performance characteristics of the storage group through the selection of appropriate storage devices. Typically, separate storage pools are created for devices of different types (e.g., NVMe, SSD, and HDD) or for specific models of these devices that have different capacity and access speed.

### Actor {#actor}

The [actor model](https://en.wikipedia.org/wiki/Actor_model) is one of the main approaches to execution parallelism used in {{ ydb-short-name }}. In this model, **actors** are lightweight user-space processes that can have and modify their private state, but can only influence each other indirectly through message passing. {{ ydb-short-name }} has its own implementation of this model, which is described [below](#actor-implementation).

In {{ ydb-short-name }}, actors with reliably persisted state are called [tablets](#tablet).

### Tablet {#tablet}

A **tablet** is one of the main building blocks and abstractions of {{ ydb-short-name }}. It is an entity responsible for a relatively small segment of user or system data. Typically, a tablet manages up to several gigabytes of data, although some types of tablets can handle larger volumes.

For example, a [row-based user table](#row-oriented-table) is managed by one or more tablets of type [DataShard](#data-shard), with each tablet responsible for a continuous range of [primary keys](#primary-key) and their corresponding data.

End users who send queries to the {{ ydb-short-name }} cluster for execution do not need to know the details of tablets, their types, or how they work, but this knowledge can be useful, for example, for performance optimization.

Technically, tablets are [actors](#actor) with state reliably stored in [distributed storage](#distributed-storage). This state allows a tablet to continue operating on another [database node](#database-node) if the previous one fails or becomes overloaded.

[Tablet implementation details](#tablet-implementation) and related terms, as well as [main tablet types](#tablet-types), are discussed below.

### Transactions {#transactions}

{{ ydb-short-name }} implements **transactions** at two main levels:

* [Local database](#local-database) and the rest of the [tablet infrastructure](#tablet-implementation) allow [tablets](#tablet) to manipulate their state using **local transactions** with [serializable isolation level](https://en.wikipedia.org/wiki/Isolation_(database_systems)#Serializable_(%D1%83%D0%BF%D0%BE%D1%80%D1%8F%D0%B4%D0%BE%D1%87%D0%B8%D0%B2%D0%B0%D0%B5%D0%BC%D0%BE%D1%81%D1%82%D1%8C)). Technically, they are not local to a single node, since this state is stored remotely in [distributed storage](#distributed-storage).
* In the context of {{ ydb-short-name }}, the term **distributed transactions** usually refers to transactions that span multiple tablets. For example, transactions between tables or even rows of a single table are often distributed.
* **Single-shard** transactions cover one tablet and execute faster. For example, transactions between rows of a single table partition are often single-shard.

These mechanisms allow {{ ydb-short-name }} to provide [strong consistency](https://en.wikipedia.org/wiki/Consistency_model#Strict_consistency).

{% if oss %}

The implementation of distributed transactions is discussed in a separate article [{#T}](../contributor/datashard-distributed-txs.md), and below is a list of several [related terms](#deterministic-transactions).

{% endif %}

### Sessions

Logical connections to the database that store the context needed for executing queries and managing transactions. Sessions are described in more detail in the [{#T}](query_execution/index.md#sessions) section.

### Client timeout {#client-timeout}

**Client timeout** or **client-side timeout** is a time limit that an application or {{ ydb-short-name }} SDK waits for a database operation to complete (for example, executing a query or receiving a response via a gRPC call). After this time expires, the client usually aborts the wait: closes the connection or data stream, receives a transport or SDK error — before the server has returned an explicit response (see {{ ydb-short-name }} server [response codes](../reference/ydb-sdk/ydb-status-codes.md)).

If the client timeout is shorter than the query execution time on the {{ ydb-short-name }} side, then due to the specifics of query processing in the cluster, a query interrupted on the client side may continue to execute on the server for some time. If such a situation occurs on a large scale, the server becomes overloaded with queries for which the client is not waiting for a response. Therefore, frequent retries of the same query immediately after a timeout can exacerbate the overload. For more details, see the articles [{#T}](../troubleshooting/performance/queries/retry-cascade.md) and [{#T}](../troubleshooting/performance/queries/overloaded-errors.md); retry policies in the SDK are described in the [{#T}](../reference/ydb-sdk/error_handling.md) section.

### Transaction retry {#transaction-retry}

**Transaction retry** is a client practice of re-executing a [transaction](#transactions) from the beginning upon a retryable error (for example, a temporary network failure or optimistic locking conflict). In {{ ydb-short-name }}, retries should be performed at the transaction level, not at the level of individual queries within it. Built-in retry policies in {{ ydb-short-name }} SDK and integrations (for example, [spring-ydb-retry](../integrations/spring/spring-retry.md)) implement this approach. For more details, see [{#T}](../reference/ydb-sdk/error_handling.md).

### Exponential backoff {#exponential-backoff}

**Exponential backoff** is a strategy of pausing between [transaction retry](#transaction-retry) attempts: the wait interval grows exponentially with each attempt, usually with an upper limit. {{ ydb-short-name }} SDK often uses two levels of backoff — fast and slow — depending on the error type. For more details, see [{#T}](../reference/ydb-sdk/error_handling.md#handling-retryable-errors).

### Jitter {#jitter}

**Jitter** is a small random variation added to the delays of [exponential backoff](#exponential-backoff). It helps avoid simultaneous retries by many clients after a common failure (a "retry storm") and distributes the load more evenly.

### Idempotency {#idempotency}

**Idempotency** is a property of an operation: repeated execution yields the same effect as a single execution (for example, `UPSERT` with a deterministic primary key or read operations). [Transaction retries](#transaction-retry) are safe only for idempotent operations or for retry errors where the server guarantees that the transaction was not committed. The {{ ydb-short-name }} SDK and client libraries may extend the set of retryable status codes if the calling code marks the operation as idempotent.

### Transaction interceptor {#transaction-interceptor}

**Transaction interceptor** is a Spring Framework component that wraps methods annotated with `@Transactional` and manages transaction boundaries. Modules like [spring-ydb-retry](../integrations/spring/spring-retry.md) replace the standard Spring interceptor, adding [transaction retry](#transaction-retry) logic around transactional methods.

### Implicit transactions {#implicit-transactions}

An **implicit transaction** is a query execution mode where the [transaction mode](transactions.md#modes) is not specified. In this case, {{ ydb-short-name }} independently determines whether to wrap them in a transaction. This mode is described in more detail in [{#T}](transactions.md#implicit).

### Multi-version concurrency control {#mvcc}

[**Multi-version concurrency control**](https://en.wikipedia.org/wiki/Multiversion_concurrency_control), or **MVCC**, is a method used by {{ ydb-short-name }} for concurrent access of multiple parallel transactions to the database without mutual interference. It is described in more detail in a separate article [{#T}](query_execution/mvcc.md).

### Streaming queries {#streaming-query}

A type of query designed for [stream processing](https://en.wikipedia.org/wiki/Stream_processing) of an unbounded data stream. Unlike regular queries, streaming queries have no execution time limits, are automatically restarted on errors, and periodically save their state as [checkpoints](#streaming-queries-checkpoints) to ensure fault tolerance.

Streaming queries are described in more detail in a separate article [{#T}](streaming-query.md).

### Streaming query checkpoints {#streaming-queries-checkpoints}

The periodically saved state of a [streaming query](#streaming-query), necessary for automatically restoring its operation after failures in a distributed system. More about checkpoints in the article [{#T}](../dev/streaming-query/checkpoints.md).

### Topology {#topology}

{{ ydb-short-name }} supports several [cluster](#cluster) **topologies**, described in more detail in a separate article [{#T}](topology.md). Several related terms are explained below.

#### Availability zones and regions {#regions-az}

An **availability zone** is a data center or its isolated segment with minimal physical distance between nodes and minimal risk of simultaneous failure with other availability zones. Thus, availability zones should not share common infrastructure such as power supply, cooling, or external network connections.

A **region** is a large geographic area containing multiple availability zones. The distance between availability zones in one region should be about 500 km or less. {{ ydb-short-name }} performs data writes to each availability zone in the region synchronously, ensuring reasonable latency and uninterrupted operation in case of failure of one of the availability zones.

#### Rack {#rack}

A **rack**, **server rack**, is equipment used to organize the placement of multiple servers. Servers in the same rack are more likely to become unavailable simultaneously due to rack-level issues related to power, cooling, etc. {{ ydb-short-name }} can take into account information about which server is in which rack when placing each data fragment in environments based on physical servers.

#### Pile {#pile}

**Pile** is a set of nodes that can fail or be disconnected simultaneously while maintaining operability of other parts of the cluster (pile). A pile can maintain operability when other cluster nodes are disconnected. Piles are used in [bridge mode](#bridge) to split the cluster into several parts between which synchronous replication is performed. A pile can consist of nodes from one or more regions.

#### Bridge mode {#bridge}

**Bridge mode** is a special cluster topology in which data is stored with synchronous replication between several [piles](#pile). The features of this mode are described in [{#T}](topology.md#bridge) and in [{#T}](bridge.md).

### Table {#table}

**Table** is a structured piece of information organized into rows and columns. Each row represents a single record or item, and each column is a specific attribute or field with a defined data type.

There are two main approaches to representing tabular data in memory or on disk: [row-oriented (row by row)](#row-oriented-table) and [column-oriented (column by column)](#column-oriented-table). The chosen approach greatly affects the performance characteristics of operations on this data: the former is more suitable for transactional workloads (OLTP), and the latter for analytical workloads (OLAP). {{ ydb-short-name }} supports both approaches.

#### Row-oriented table {#row-oriented-table}

**Row-oriented tables** store data for all or most columns of each row physically close together. They are described in more detail in [{#T}](datamodel/table.md#row-oriented-tables).

#### Column-oriented table {#column-oriented-table}

**Column-oriented tables** or **columnar tables** store data for each column separately. They are optimized for building aggregates over a small number of columns, but are less suitable for accessing specific rows, as rows need to be reconstructed from their cells on the fly. They are described in more detail in [{#T}](datamodel/table.md#column-oriented-tables).

#### Primary key {#primary-key}

**Primary key** is an ordered list of columns whose values uniquely identify a row. It is used to create the table's [primary index](#primary-index). It is set by the {{ ydb-short-name }} user when [creating a table](../yql/reference/syntax/create_table/index.md) and significantly affects the performance of operations on that table.

Guidance on choosing primary keys is provided in [{#T}](../dev/primary-key/index.md).

#### Primary index {#primary-index}

**Primary index** or **primary key index** is the main data structure used to find rows in a table. It is created based on the selected [primary key](#primary-key) and determines the physical order of rows in the table; thus, each table can have only one primary index. The primary index is unique.

#### Secondary index {#secondary-index}

**Secondary index** is an additional data structure used to find rows in a table, typically when this cannot be done efficiently using the [primary index](#primary-index). Unlike the primary index, secondary indexes are managed independently of the table's main data. Thus, a table can have multiple secondary indexes for different scenarios. {{ ydb-short-name }} capabilities regarding secondary indexes are described in a separate article [{#T}](query_execution/secondary_indexes.md). A secondary index can be either unique or non-unique.

Special types of secondary indexes are distinguished: [vector index](#vector-index), [full-text index](#fulltext-index), and [JSON index](#json-index).

#### Vector index {#vector-index}

**Vector index** is an additional data structure used to speed up solving the [vector search](query_execution/vector_search.md) problem when there is a lot of data and [exact vector search without an index](../yql/reference/udf/list/knn.md) does not work satisfactorily.
{{ ydb-short-name }} capabilities for approximate nearest neighbor search (ANN search) using vector indexes are described in a separate article [{#T}](../dev/vector-indexes.md).

**Vector index** is a specialized type of [secondary index](#secondary-index), designed for similarity search, unlike traditional secondary indexes optimized for equality or range search.

#### Full-text index {#fulltext-index}

**Full-text index** is an additional data structure used to speed up text search on a table column (by words and phrases, and when using N-grams, also by substrings).

The full-text search capabilities and index parameters are described in the articles [{#T}](../dev/fulltext-indexes.md) and [{#T}](query_execution/fulltext_search.md).

#### JSON index {#json-index}

**JSON index** is an additional data structure used to speed up predicates with functions [JSON_EXISTS](../yql/reference/builtins/json.md#json_exists) and [JSON_VALUE](../yql/reference/builtins/json.md#json_value) on a column of type `Json` or `JsonDocument`. Unlike traditional secondary indexes optimized for equality or range searches on individual table columns, a JSON index works with arbitrary paths [JsonPath](../yql/reference/builtins/json.md#jsonpath) within a JSON document.

The JSON index, like the [full-text index](#fulltext-index), is implemented on top of an [inverted index](https://en.wikipedia.org/wiki/Inverted_index), but uses its own tokenizer for JSON documents. JSON search capabilities are described in the articles [{#T}](../dev/json-indexes.md) and [{#T}](query_execution/json_search.md).

#### Local index {#local-index}

A local index is an auxiliary structure that is stored together with the table data (unlike a [global secondary index](#secondary-index), which materializes a separate index table). A local index is used when reading the main table on the storage side. For more information: [local indexes](query_execution/local_indexes.md).

#### Bloom filter {#bloom-filter}

Bloom filter — [probabilistic data structure](https://en.wikipedia.org/wiki/Bloom_filter) that allows quickly checking whether an element belongs to a set. False positives are possible, but false negatives are not.

#### Local Bloom index {#local-bloom-skip-index}

A local Bloom index is a special case of a [local index](#local-index): a probabilistic filter over column values based on a [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter), which speeds up selective queries by skipping data fragments where the searched value is guaranteed to be absent. For more information: [Bloom indexes](../dev/bloom-skip-indexes.md), [local indexes](query_execution/local_indexes.md).

#### Column family {#column-family}

**Column family** or **column group** is a feature that allows storing subsets of columns of a [row table](#row-oriented-table) separately in a dedicated family or group. The main use case is storing some columns on other disk types (moving less important columns to HDD) or with different compression settings. If your workload requires many column families, consider using [column tables](#column-oriented-table).

#### Column encoding {#column-encoding}

**Column encoding** is a data storage optimization mechanism for table columns that reduces disk space usage and speeds up the execution of certain operations.

#### Time to Live {#ttl}

**Time to live**, **TTL** — this is a mechanism for automatically deleting old rows from a table asynchronously in the background. It is described in a separate article [{#T}](ttl.md).

### View {#view}

**View** is a way to save a query and access its results as if they were a real table. The view itself does not store any data except the query text. The query stored in the view is executed each time a SELECT is performed on it, generating the returned result. Any changes to the tables referenced by the view are immediately reflected in the results of reading from it.

{% if feature_view %}

Views can be user-defined or system.

#### User views {#user-view}

**User views** are created by the user using the [{#T}](../yql/reference/syntax/create-view.md) command. They are described in more detail in [{#T}](../concepts/datamodel/view.md).

{% endif %}

#### System views {#system-view}

**System views** are special views automatically created by the system for monitoring the state of a database and cluster. They are located in a special directory `.sys` in the root folder of each database. System views for databases are described in [{#T}](../dev/system-views.md); system views for the cluster and access management are described in [{#T}](../devops/observability/system-views.md).

### Topic {#topic}

A **message queue** is used for reliable asynchronous communication between different systems by passing messages. {{ ydb-short-name }} provides infrastructure that ensures exactly-once semantics in such communications. Using it, you can achieve a guarantee of no lost messages or accidental duplicates.

A **topic** is a named entity in a message queue designed for interaction between [writers](#producer) and [readers](#consumer).

Several terms related to topics are given below. How {{ ydb-short-name }} topics work is explained in more detail in a separate article [{#T}](datamodel/topic.md).

#### Partition {#partition}

For horizontal scaling, topics are divided into individual elements called **partitions**. Thus, partitions are the unit of parallelism within a topic. Messages within each partition are ordered.

However, subsets of data managed by a single [data shard](#data-shard) or [column shard](#column-shard) may also be called partitions.

#### Offset {#offset}

An **offset** is a sequence number that identifies a message within a [partition](#partition).

#### Writer {#producer}

A **writer** or **producer** is an entity that writes new messages to a topic.

#### Reader {#consumer}

A **reader** or **consumer** is an entity that reads messages from a topic.

### Change Data Capture {#cdc}

**Change data capture** or **CDC** is a mechanism that allows you to subscribe to a **change feed** in a specific [table](#table). Technically, it is implemented on top of [topics](#topic). It is described in more detail in a separate article [{#T}](cdc.md).

#### Change Feed {#changefeed}

A **change feed** is an ordered list of changes to a [table](#table) placed in a [topic](#topic).

### Backup Collection {#backup-collection}

A **backup collection** is a [schema object](#scheme-object) that organizes full and incremental [backups](#backup) for selected [row tables](#row-oriented-table). Collections provide [point-in-time recovery](https://en.wikipedia.org/wiki/Point-in-time_recovery) by maintaining [backup chains](#backup-chain) and ensuring consistent recovery of multiple tables. A table can belong to only one backup collection at a time.

For more information, see [{#T}](datamodel/backup-collection.md).

#### Backup {#backup}

A **backup** is a copy of data at a specific point in time that can be used for data recovery. In the context of [backup collections](#backup-collection), there are two types:

- **Full backup**: A complete snapshot of all data in the collection. Serves as the basis for [backup chains](#backup-chain) and can be restored independently.
- **Incremental backup**: Captures only changes (inserts, updates, deletes) since the previous backup. Requires the entire backup chain for restoration.

#### Backup Chain {#backup-chain}

A **backup chain** is an ordered sequence of [backups](#backup) starting with a full backup followed by zero or more incremental backups. Each incremental backup depends on all previous backups in the chain. Deleting any backup in the chain makes subsequent incremental backups unrecoverable.

{% if feature_async_replication == true %}

### Asynchronous Replication Instance {#async-replication-instance}

An **asynchronous replication instance** is a named entity that stores [asynchronous replication](async-replication.md) settings (connection settings, list of replicated objects, etc.). It can also be used to obtain information about the state of asynchronous replication: [initial scan progress](async-replication.md#initial-scan), [lag](async-replication.md#replication-of-changes), [errors](async-replication.md#error-handling), etc.

#### Replicable object {#replicated-object}

**Replicable object** — an object (e.g., a table) for which asynchronous replication is configured.

#### Replica object {#replica-object}

**Replica object** — a mirror copy of the replicable object, automatically created by an asynchronous replication instance. Typically, it is read-only.

{% endif %}

{% if feature_transfer == true %}

### Transfer instance {#transfer-instance}

**Transfer instance** is a named entity that stores [transfer](transfer.md) settings, including connection settings and data transformation rules. It can also be used to obtain information about the transfer status, such as [errors](transfer.md#error-handling).

{% endif %}

### Coordination node {#coordination-node}

**Coordination node** is a schema object that allows client applications to create semaphores for coordinating their actions. Coordination nodes are used to implement distributed locks, service discovery, leader election, and other scenarios. For more details, see [coordination nodes](./datamodel/coordination-node.md).

#### Semaphore {#semaphore}

**Semaphore** is an object inside a [coordination node](#coordination-node) that provides a synchronization mechanism for distributed applications. Semaphores can be permanent or temporary and support create, acquire, release, and monitor operations. For more details, see [semaphores in {{ ydb-short-name }}](./datamodel/coordination-node.md#semaphore).

{% if feature_resource_pool == true and feature_resource_pool_classifier == true %}

### Resource pool {#resource-pool}

**Resource pool** is a schema object that describes the constraints imposed on resources (CPU, RAM, etc.) available for executing queries in this resource pool. A query is always executed in some resource pool. By `default`, all queries are executed in a resource pool named , which does not impose any restrictions. For more details on using resource pools, see [{#T}](../dev/resource-consumption-management.md).

### Resource pool classifier {#resource-pool-classifier}

**Resource pool classifier** is an object designed to manage the distribution of queries among [resource pools](#resource-pool). It describes the rules by which a resource pool is selected for each query. These classifiers are global for the entire [database](#database) and apply to all queries that come into it. For more details on their usage, see [{#T}](../dev/resource-consumption-management.md).

{% endif %}

### YQL {#yql}

**YQL ({{ ydb-short-name }} Query Language)** is a high-level language for working with the system. It is a dialect of [ANSI SQL](https://en.wikipedia.org/wiki/SQL). There are many materials dedicated to YQL, including a [tutorial](../dev/yql-tutorial/index.md), [reference guide](../yql/reference/syntax/index.md), and [recipes](../yql/reference/recipes/index.md).

### Federated queries {#federated-queries}

**Federated queries** is a feature that allows executing queries against data stored in systems external to the {{ ydb-short-name }} cluster.

Below are explanations of several terms related to federated queries. How federated queries work in {{ ydb-short-name }} is explained in more detail in a separate article [{#T}](query_execution/federated_query/index.md).

#### External data source {#external-data-source}

**External data source**, **external connection** — these are metadata that describe how to connect to a supported external system to execute [federated queries](#federated-queries).

#### External table {#external-table}

**External table** is metadata that describes a specific dataset that can be retrieved from an [external data source](#external-data-source).

#### Secret {#secret}

**Secret** is confidential metadata that requires special handling. For example, secrets can be used in definitions of [external data sources](#external-data-source) and represent entities such as passwords and tokens.

### Authentication token {#auth-token}

**Authentication token** or **auth token** — a token used for [authentication](../security/authentication.md) in {{ ydb-short-name }}.

{{ ydb-short-name }} supports [different authentication types](../security/authentication.md) and various token types.

### Cluster schema {#scheme}

**{{ ydb-short-name }} cluster schema** is the hierarchical namespace of the {{ ydb-short-name }} cluster. The top-level element of this namespace is the [cluster schema root](#scheme-root). The child elements of the cluster schema root are [databases](#database). Inside each database, you can create an arbitrary hierarchy of [objects](#scheme-object) (tables, topics, etc.) using nested directories.

### Database schema {#scheme-database}

**Database schema** is a subset of the cluster's hierarchical namespace that belongs to a database.

### Database root {#scheme-database-root}

**Database root** is the path to a database in the cluster schema.

### Schema root {#scheme-root}

**Cluster schema root** is the root element of the [{{ ydb-short-name }} namespace](datamodel/cluster-namespace.md), whose child elements are [databases](#database).

### Schema object {#scheme-object}

A database schema consists of **schema objects**, which can be databases, [tables](#table) (including [external tables](#external-table)), [topics](#topic), [folders](#folder), etc.

For organizational convenience, schema objects form a hierarchy using [folders](#folder).

### Folder {#folder}

As in file systems, a **folder**, **catalog**, **folder**, or **directory** is a container for [schema objects](#scheme-object).

Folders can contain subfolders, and such nesting can be of arbitrary depth.

### Access object {#access-object}

An **access object** is an entity for which access rights and restrictions are configured during [authorization](../security/authorization.md). In {{ ydb-short-name }}, access objects are [schema objects](#scheme-object).

Each [schema object](#scheme-object) has an [owner](#access-owner) and an [access control list](#access-control-list) for that object, granted to users and groups ([access subjects](#access-subject)).

### Access subject {#access-subject}

An **access subject** is an entity that can access [access objects](#access-object) and perform certain actions in the system.

Gaining access during these requests and actions depends on the configured [access control lists](#access-control-list) and the subject's [access level](#access-level).

An access subject can be a [user](#access-user) or a [group](#access-group).

### Access right {#access-right}

An ** [access right](../security/authorization.md#right)** is an entity that reflects permission for an [access subject](#access-subject) to perform a specific set of operations in a cluster or database on a specific [access object](#access-object).

### Inheritance of access rights {#access-right-inheritance}

**Inheritance of access rights** is a mechanism where [access rights](#access-right) granted on parent [access objects](#access-object) are inherited by child objects in the hierarchical database structure. This ensures that permissions granted at a higher level of the hierarchy apply to all lower levels unless they are [explicitly overridden](../reference/ydb-cli/commands/scheme-permissions.md#clear-inheritance).

### Access control list {#access-control-list}

An ** [access control list](../security/authorization.md#right)** (ACL) is a list of all [rights](#access-right) granted to [access subjects](#access-subject) (users and groups) on a specific [access object](#access-object).

### Access level {#access-level}

An **access level** provides an [access subject](#access-subject) with additional capabilities when working with [schema objects](#scheme-object), as well as the ability to perform operations on the cluster as a whole. {{ ydb-short-name }} uses hierarchical access levels:

- Database.
- Viewer.
- Monitoring.
- Administration.

The access level for a subject is configured using [access level lists](#access-level-list).

### Access level list {#access-level-list}

An **access level list** or **permission list** is a list of [SID](#access-sid)s of [access subjects](#access-subject) that are allowed a specific [access level](#access-level).

In {{ ydb-short-name }}, there are [several such lists](../reference/configuration/security_config.md#security-access-levels) that define who has which [access levels](#access-level).

Detailed information about access level lists, their hierarchy, and operating principles is provided in the [Access Level Lists](../security/authorization.md#access-level-lists) section of the authorization documentation.

### Owner {#access-owner}

An ** [owner](../security/authorization.md#owner)** is an [access subject](#access-subject) ([user](#access-user) or [group](#access-group)) that has full rights to a specific [access object](#access-object).

### User {#access-user}

A ** [user](../security/authorization.md#user)** is a person who uses {{ ydb-short-name }} to perform a specific function.

In {{ ydb-short-name }}, there are different types of users depending on the creation method:

- Local users in {{ ydb-short-name }} databases.
- External users from third-party directories.

A user is identified by a [SID](#access-sid).

#### Local user {#local-user}

A user whose account is created directly in {{ ydb-short-name }} using the YQL command `CREATE USER` or during [initial security configuration](../security/builtin-security.md).

#### External user {#external-user}

A {{ ydb-short-name }} user whose account is created in a third-party directory, for example, an LDAP directory or IAM system.

### Group {#access-group}

**[Group](../security/authorization.md#group)** or **access group** is a named set of [users](#access-user) and other groups with equal capabilities for their members.

A group is identified by a [SID](#access-sid).

### Role {#access-role}

A role is a named set of [access rights](#access-right) used to assign to [users](#access-user) or [groups](#access-group) of users.

Roles in {{ ydb-short-name }} are implemented using [groups](#access-group), which are created during the initial deployment of the cluster, and to which a specific [list of rights](#access-right) is assigned at the root of the cluster schema. For more details about roles, see the article [{#T}](../security/builtin-security.md).

### SID {#access-sid}

**SID** or **security identifier** is a string of the form `<name>` or `<name>@<auth-domain>` that identifies an [access subject](../concepts/glossary.md#access-subject). It is used in [authentication](../security/authentication.md), [authorization](../security/authorization.md), [access lists](#access-control-list), and [access level lists](#access-level-list).

A SID identifies an individual [user](#access-user) or [group of users](#access-group).

The optional suffix `@<auth-domain>` identifies the source of the access subject, i.e., the external directory or system from which it was obtained. For example, users or groups from an LDAP directory may have the suffix `@ldap`. The absence of a suffix means that the user or group is created and exists directly in {{ ydb-short-name }}.

### Query optimizer {#optimizer}

[**Query optimizer**](https://en.wikipedia.org/wiki/Query_optimization) is a set of {{ ydb-short-name }} components responsible for converting the logical representation of a query into a specific physically executable plan for obtaining the requested result. The main goal of the optimizer is to select, among all possible query execution plans, one that is sufficiently efficient in terms of predicted execution time and cluster resource consumption. It is described in more detail in a separate article [{#T}](query_execution/optimizer.md).

### Compilation cache {#compile-cache}

**Compilation cache** or **compile cache** is a cache of compiled queries on each [node](#node) of the cluster. It is used to avoid recompilation: if the query text is already in the node's cache, no additional compilation is performed. For more details, see the section [Query compilation cache](../dev/system-views.md#compile-cache-queries).

## Advanced terminology {#advanced-terminology}

This section explains terms that are useful for [{{ ydb-short-name }} contributors](../contributor/index.md) and users who want to understand more deeply what happens inside the system.

### Actor implementation {#actor-implementation}

#### Actor system {#actor-system}

**Actor system** or **actor system** is a C++ library with an [implementation](https://github.com/ydb-platform/ydb/tree/main/ydb/library/actors) of the [actor model](https://en.wikipedia.org/wiki/Actor_model) for the needs of {{ ydb-short-name }}.

#### Actor service {#actor-service}

**Actor service** or **actor service** is an [actor](#actor) that has a known name and is usually executed in a single instance on a [node](#node).

#### ActorId {#actorid}

**ActorId** is a unique identifier of an actor or a [tablet](#tablet) in a [cluster](#cluster).

#### Actor system interconnect {#actor-system-interconnect}

**Actor system interconnect**, **interconnect**, **actor system interconnect**, or **interconnect** is the internal network layer of a [cluster](#cluster). All [actors](#actor) interact with each other in the system through the interconnect.

#### Local {#local}

**Local** is an [actor service](#actor-service) running on each [node](#node). It directly manages [tablets](#tablet) on its node and interacts with [Hive](#hive). It registers with Hive and receives commands to start tablets.

### Tablet implementation {#tablet-implementation}

[**Tablet**](#tablet) is an [actor](#actor) with persistent state. It includes a set of data for which this tablet is responsible, and a state machine through which the data (or state) of the tablet is changed. The tablet is a fault-tolerant entity because the tablet data is stored in a [distributed storage](#distributed-storage) that survives disk and node failures. The tablet automatically restarts on another [node](#node) in case of failure or overload of the previous one. Data in the tablet is changed sequentially, as the system infrastructure guarantees that there is no more than one [tablet leader](#tablet-leader) through which changes to the tablet data are performed.

A tablet solves the same problem as the [Paxos](https://en.wikipedia.org/wiki/Paxos_(computer_science)) and [Raft](https://en.wikipedia.org/wiki/Raft_(algorithm)) algorithms in other systems, namely the problem of [distributed consensus](https://en.wikipedia.org/wiki/Consensus_(computer_science)). From a technical perspective, a tablet implementation can be described as a replicated state machine (RSM) on top of a shared log, since the tablet state is fully described by an ordered command log stored in a distributed and fault-tolerant storage.

During execution, the tablet state machine is managed by three components:

1. The common tablet part ensures log consistency and recovery in case of failures.
2. **Executor** is an abstraction of a local database, namely data structures and code that organize work with data stored by the tablet.
3. An actor with user code implementing the specific logic of a particular tablet type.

In {{ ydb-short-name }}, there are several types of specialized tablets storing various data for different tasks. Many {{ ydb-short-name }} features, such as [tables](#table) and [topics](#topic), are implemented as different tablet types. Thus, reusing the tablet infrastructure is one of the key means of extensibility of {{ ydb-short-name }} as a platform.

Typically, a {{ ydb-short-name }} cluster runs orders of magnitude more tablets compared to the processes or threads that other systems would use for a cluster of similar size. In a {{ ydb-short-name }} cluster, hundreds of thousands or millions of tablets can easily run simultaneously.

Since a tablet stores its state in [distributed storage](#distributed-storage), it can be (re)started on any cluster node. Tablets are identified by a [TabletID](#tabletid), a 64-bit number assigned when the tablet is created.

### Tablet leader {#tablet-leader}

**Tablet leader** is the current active leader of a given tablet. The tablet leader accepts commands, assigns them an order, and confirms them to the outside world. It is guaranteed that at any given time, there is at most one leader for each tablet.

### Tablet candidate {#tablet-candidate}

**Tablet candidate** is one of the election participants that wants to become the [leader](#tablet-leader) of a given tablet. If the candidate wins the election, it becomes the tablet leader.

### Tablet replica {#tablet-follower}

**Tablet replica**, **hot standby**, **tablet follower**, or **hot standby** is a copy of the [tablet leader](#tablet-leader) that applies the command log accepted by the leader (with some delay). A tablet can have zero or more replicas. Replicas perform two main functions:

* In case of leader termination or failure, replicas are preferred [candidates](#tablet-candidate) for the new leader role, as they can become leader much faster than other candidates because they have applied most of the log.
* Replicas can respond to read-only requests if the client explicitly selects an optional relaxed transaction mode that allows stale reads.

### Tablet generation {#tablet-generation}

**Tablet generation** is a number identifying the reincarnation of the tablet leader. It changes only when a new leader is elected and always increases.

### Tablet local database {#local-database}

**Tablet local database**, **local database**, **tablet local database**, or **local database** is a set of data structures and associated code that manage the tablet state and the data it stores. Logically, the local database state is represented by a set of tables, very similar to relational tables. Modification of the local database state is performed by tablet local transactions created by the tablet user actor.

Each local database table is stored as an [LSM tree](#lsm-tree).

#### Log-structured merge-tree {#lsm-tree}

** [Log-structured merge-tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree)** or **LSM tree** is a data structure designed to optimize write and read performance in storage systems. It is used in {{ ydb-short-name }} to store [local database](#local-database) tables and [VDisks](#vdisk) data.

#### MemTable {#memtable}

All data written to tables of the [local database](#local-database) is initially stored in an in-memory data structure called a **MemTable**. When the MemTable reaches a specified size, it is flushed to disk as an immutable data structure [SST](#sst).

#### Sorted string table {#sst}

**Sorted string table** or **SST** is an immutable data structure that stores table rows sorted by key, which facilitates efficient key lookup and range scans. Each SST consists of a contiguous series of small data pages, typically about 7 KiB each, further optimizing disk read performance. An SST is usually part of an [LSM tree](#lsm-tree).

#### Tablet pipe {#tablet-pipe}

**Tablet pipe** or **TabletPipe** is a virtual connection that can be established with a tablet. It involves locating the [tablet leader](#tablet-leader) by [TabletID](#tabletid). This is the recommended way to interact with a tablet. The term **open a pipe to a tablet** describes the process of resolving (locating) a tablet in the cluster and establishing a virtual communication channel with it.

#### TabletID {#tabletid}

**TabletID** is a unique identifier of a [tablet](#tablet) within a cluster.

#### Bootstrapper {#bootstrapper}

**Bootstrapper** is the primary mechanism for starting tablets, used for system tablets (e.g., [Hive](#hive), [DS controller](#ds-controller), root [SchemeShard](#scheme-shard)). [Hive](#hive) initializes the remaining tablets.

### Shared cache {#shared-cache}

**Shared cache** is an [actor](#actor) that stores data pages recently read from [distributed storage](#distributed-storage). Caching these pages reduces disk I/O operations and speeds up data retrieval, improving overall system performance.

### Memory controller {#memory-controller}

**Memory controller** is an [actor](#actor) that manages [memory limits](../reference/configuration/memory_controller_config.md) of {{ ydb-short-name }}.

### Spilling {#spilling}

**Spilling** is a memory management mechanism in {{ ydb-short-name }} that temporarily offloads intermediate query data to external storage when such data exceeds the available RAM of a node. In {{ ydb-short-name }}, disk is currently used for spilling.

For more details about spilling, see [{#T}](query_execution/spilling.md).

### Tablet types {#tablet-types}

[Tablets](#tablet) can be considered a framework for building reliable components that operate in a distributed system. Many components of {{ ydb-short-name }} — both system and user-data components — are implemented using this framework; the main ones are listed below.

#### SchemeShard {#scheme-shard}

**SchemeShard** or **Scheme shard** is a system tablet that stores the database schema, including metadata of user [tables](#table), [topics](#topic), etc.

Additionally, there is a **root SchemeShard** that stores information about databases created in the cluster.

#### DataShard {#data-shard}

**DataShard** or **Data shard** is a tablet that manages a segment of a [row-based user table](datamodel/table.md#row-oriented-tables). A logical user table is divided into segments by contiguous ranges of the table's primary key. Each such range is handled by a separate DataShard tablet. The range itself is also called a [partition](#partition). The DataShard tablet stores data row by row, which is efficient for OLTP workloads.

#### ColumnShard {#column-shard}

**ColumnShard** or **Column shard** is a tablet that stores a data segment of a [column-based user table](datamodel/table.md#column-oriented-tables).

#### KeyValue Tablet {#kv-tablet}

**KeyValue**, **KV Tablet**, or **key-value tablet** is a tablet that implements a simple key → value mapping, where keys and values are strings. It also has several specific features, such as locks.

#### PersQueue Tablet {#pq-tablet}

**PersQueue** or **persistent queue tablet** is a tablet that implements the concept of a [topic](#topic). Each topic consists of one or more partitions, and each partition is managed by a separate instance of the PQ tablet.

#### TxAllocator {#txallocator}

**TxAllocator**, **transaction allocator** — it is a system tablet that allocates unique transaction identifiers ( [TxID](#txid)) in the cluster. Typically, there are several such tablets in the cluster, from which the [transaction proxy](#transaction-proxy) pre-allocates and caches ranges for local issuance within a single process.

#### Coordinator {#coordinator}

**Coordinator** — it is a system tablet that ensures the global order of transactions. The coordinator's task is to assign a logical time [PlanStep](#planstep) to each transaction planned through this coordinator. Each transaction is assigned exactly one coordinator, selected by hashing its [TxId](#txid).

#### Mediator {#mediator}

**Mediator** — it is a system tablet that distributes transactions planned by [coordinators](#coordinator) among transaction participants. Mediators ensure the advancement of global time. Each transaction participant is associated with exactly one mediator. Mediators avoid the need for a full set of connections between all coordinators and all participants of all transactions.

#### Hive {#hive}

**Hive** — it is a system tablet responsible for starting and managing other tablets. Its responsibilities include moving tablets between nodes in case of a [node](#node) failure or overload.{% if audience != "corp" %} More details about Hive can be found in a [separate article](../contributor/hive.md).{% endif %}

#### CMS {#cms}

**CMS**, **cluster management system** — it is a system tablet responsible for managing information about the current state of the [{{ ydb-short-name }} cluster](#cluster). This information is used to perform gradual cluster restarts without affecting user workloads, maintenance, cluster reconfiguration, etc.

#### NodeBroker {#node-broker}

**NodeBroker** — it is a system tablet that is responsible for registering [dynamic nodes](#dynamic) in the cluster.

#### BSController {#ds-controller}

**BSController**, **blob storage controller** — it manages the dynamic configuration of the distributed storage, including information about [PDisk](#pdisk), [VDisk](#vdisk), and [storage groups](#storage-group). It interacts with [node warden](#node-warden) to start various components of the distributed storage. It interacts with [Hive](#hive) to allocate [channels](#channel) to tablets.

#### Console {#console}

**Console** — it is a system tablet responsible for storing [dynamic configuration](../devops/configuration-management/configuration-v1/dynamic-config.md) and delivering it to cluster nodes.

#### Kesus {#kesus}

**Kesus** — it is a tablet that implements a [coordination node](datamodel/coordination-node.md).

#### SysViewProcessor {#sys-view-processor}

**SysViewProcessor** — it is a tablet that stores data of some [system views](../dev/system-views.md).

{% if feature_serial %}

#### SequenceShard {#sequence-shard}

**SequenceShard** — it is a tablet that serves Sequence objects, which are used to implement [serial data types](../yql/reference/types/serial.md).

{% endif %}

{% if feature_async_replication %}

#### ReplicationController {#replication-controller}

**ReplicationController** — it is a tablet responsible for the [asynchronous replication](async-replication.md) process.

{% endif %}

#### StatisticsAggregator {#statistics-aggregator}

**StatisticsAggregator** — it is a tablet responsible for collecting statistics used in cost optimization.

### Slot {#slot}

**Slot** in {{ ydb-short-name }} can be used in two contexts:

* **Slot** — it is a portion of server resources allocated for running one {{ ydb-short-name }} [node](#node). The typical slot size is 10 CPU cores and 50 GB of RAM. Slots are used if the {{ ydb-short-name }} cluster is deployed on servers or virtual machines with sufficient resources to host multiple slots.
* **VDisk slot** or **VSlot** — it is a share of [PDisk](#pdisk) that can be allocated to one of the [VDisk](#vdisk).

### State storage {#state-storage}

**State storage** or **StateStorage** — it is a distributed service that stores information about tablets, namely:

* The current tablet leader or its absence.
* Tablet replicas.
* Tablet generation and step `(generation:step)`.

State storage is used as a service for resolving tablet names, i.e., to obtain [ActorId](#actorid) by [TabletID](#tabletid). StateStorage is also used in the process of selecting a [tablet leader](#tablet-leader).

Information in the state storage is volatile. Thus, it is lost when power is disconnected or the process is restarted. Despite its name, this service is not a permanent long-term storage. It only contains information that is easy to recover and that should not be durable. However, the state storage stores information on multiple nodes to minimize the impact of node failures. This service can also be used to gather a quorum, which is used for selecting tablet leaders.

Due to its nature, the state storage service operates on a best-effort basis. For example, the absence of multiple tablet leaders is guaranteed through the leader election protocol on [distributed storage](#distributed-storage), not on the state storage.

### Board {#board}

**Board** is a distributed service designed for storing metadata in the form of key-value pairs. It is used, among other things, for storing information about [endpoints](../concepts/connect.md#endpoint).

### SchemeBoard {#scheme-board}

**SchemeBoard** is a distributed service designed for storing metadata in the form of key-value pairs. It is used, among other things, for storing information about [schemas](#global-schema).

#### Compaction {#compaction}

**Compaction**, **compactization**, or **compaction** is an internal background process of rebuilding the data of the [LSM tree](#lsm-tree). Data in [VDisk](#vdisk) and [local databases](#local-database) is organized as LSM trees. Therefore, **VDisk compaction** and **tablet compaction** are distinguished. The compaction process is usually quite resource-intensive, so measures are taken to minimize the overhead associated with it, for example, by limiting the number of simultaneously executed compactions.

#### gRPC proxy {#grpc-proxy}

**gRPC proxy** is a proxy system for external user requests. Client requests enter the system via the [gRPC](https://grpc.io) protocol, then the proxy component translates them into internal calls to execute these requests, transmitted via [interconnect](#actor-system-interconnect). This proxy provides an interface for both request-response and bidirectional streaming data.

### Distributed configuration {#distributed-configuration}

**Distributed configuration** or **DistConf** is an internal mechanism for [cluster configuration](../devops/configuration-management/configuration-v2/config-overview.md) that ensures the startup and configuration of [static nodes](#static-node), automatic management of the [static storage group](#static-group) and [State Storage](../concepts/glossary.md#state-storage). Distributed configuration starts before any [tablets](#tablet), [storage groups](#storage-group), and [State Storage](../concepts/glossary.md#state-storage).

More details about the distributed configuration structure are described in [{#T}](../contributor/configuration-v2.md).

### Distributed storage implementation {#distributed-storage-implementation}

**Distributed storage** is a distributed fault-tolerant data storage layer that stores binary records called [LogoBlob](#logoblob), addressed using a specific type of identifier called [LogoBlobID](#logoblobid). Thus, distributed storage is a key-value store that maps LogoBlobID to a string of up to 10 MB. Distributed storage consists of many [storage groups](#storage-group), each of which is an independent data repository.

Distributed storage stores immutable data, with each immutable data block identified by a specific LogoBlobID key. The distributed storage API is very specific, intended only for use by [tablets](#tablet) to store their data and change logs. Thus, it is not intended for general-purpose data storage. Data in distributed storage is deleted using special barrier commands. Due to the absence of mutations in its interface, distributed storage can be implemented without implementing [distributed consensus](https://en.wikipedia.org/wiki/Consensus_(computer_science)). Distributed storage is only one of the components that tablets use to implement distributed consensus.

#### LogoBlob {#logoblob}

**LogoBlob** is a set of binary immutable data identified by [LogoBlobID](#logoblobid) and stored in a [distributed storage](#distributed-storage). The data block size is limited at the [VDisk](#vdisk) level and above in the stack. Currently, the maximum data block size that VDisks can handle is 10 MB.

#### LogoBlobID {#logoblobid}

**LogoBlobID** is an identifier of a [LogoBlob](#logoblob) in a [distributed storage](#distributed-storage). It has a structure of the form `[TabletID, Generation, Step, Channel, Cookie, BlobSize, PartID]`. The main elements of LogoBlobID:

* `TabletID` is the [ID](#tabletid) of the tablet to which the LogoBlob belongs.
* `Generation` is the generation of the tablet in which the data block was written.
* `Channel` is the [channel](#channel) of the tablet on which the LogoBlob is written.
* `Step` is an incremental counter, usually within the tablet generation.
* `Cookie` is a unique identifier of a data block within a single `Step`. Cookie is usually used when writing multiple data blocks into one `Step`.
* `BlobSize` is the size of the LogoBlob.
* `PartID` is the identifier of a part of the data block. It is important when the original LogoBlob is split into parts using [erasure coding](#erasure-coding), and the parts are written to the corresponding [VDisk](#vdisk) and [storage groups](#storage-group).

#### Replication {#replication}

**Replication** is a process that ensures a sufficient number of copies (replicas) of data to maintain the desired availability characteristics of the {{ ydb-short-name }} cluster. It is typically used in geo-distributed {{ ydb-short-name }} clusters.

#### Erasure coding {#erasure-coding}

[**Erasure coding**](https://en.wikipedia.org/wiki/Erasure_code) is a data encoding method in which the original data is supplemented with redundancy and split into several fragments, enabling recovery of the original data if one or more fragments are lost. It is widely used in {{ ydb-short-name }} clusters with a single [availability zone](#regions-az), as opposed to [replication](#replication) with 3 replicas. For example, the most popular erasure coding scheme 4+2 provides the same reliability as three replicas, with a space overhead of 1.5 versus 3.

#### PDisk {#pdisk}

**PDisk**, **physical disk** is a component that controls a physical disk drive (block device). In other words, PDisk is a subsystem that implements an abstraction similar to a specialized file system on top of block devices (or files emulating a block device for testing purposes). PDisk provides data integrity control (including [erasure coding](#erasure-coding) of sector groups to recover data on individual damaged sectors, integrity control using checksums), transparent encryption of all data on the disk, and transactional guarantees for disk operations (write confirmation strictly after `fsync`).

PDisk contains a scheduler that ensures shared use of the device bandwidth among multiple clients ( [VDisk](#vdisk)). PDisk divides the block device into blocks called [slots](#slot) (about 128 megabytes in size; smaller blocks are also allowed). At any given time, no more than one VDisk can own each slot. PDisk also maintains a recovery log shared by the PDisk service records and all VDisks.

#### VDisk {#vdisk}

**VDisk**, **virtual disk** is a component that implements data storage of [distributed storage](#distributed-storage) [LogoBlob](#logoblob) on [PDisk](#pdisk). VDisk stores all its data on PDisk. One VDisk corresponds to one PDisk, but typically several VDisks are associated with one PDisk. Unlike PDisk, which hides blocks and logs behind it, VDisk provides an interface at the LogoBlob and [LogoBlobID](#logoblobid) level, for example, writing a LogoBlob, reading data by LogoBlobID, and deleting a set of LogoBlobs using a special command. VDisk is a member of a [storage group](#storage-group). The VDisk itself is local, but many VDisks in a given group provide reliable data storage. VDisks in a group synchronize data with each other and replicate data in case of losses. The set of VDisks in a storage group forms a distributed RAID.

#### Yard {#yard}

**Yard** is the name of the [PDisk](#pdisk) API. It allows a [VDisk](#vdisk) to read and write data to blocks and journals, reserve blocks, delete blocks, and transactionally acquire and release block ownership. In some contexts, Yard can be considered a synonym for PDisk.

#### Skeleton {#skeleton}

**Skeleton** is an [actor](#actor) that provides an interface to a [VDisk](#vdisk).

#### SkeletonFront {#skeletonfront}

**SkeletonFront** is a proxy actor for Skeleton that controls the flow of messages coming into Skeleton.

#### Proxy {#ds-proxy}

**Distributed storage proxy**, **DS proxy**, **BS proxy**, **distributed storage proxy**, **DS-proxy**, or **BS-proxy** acts as a client library for performing operations with [distributed storage](#distributed-storage). The users of the DS proxy are [tablets](#tablet) that write to and read from distributed storage. The DS proxy hides the distributed nature of distributed storage from the user. The task of the DS proxy is to write to a quorum of [VDisk](#vdisk), retry if necessary, and control the write/read flow to prevent VDisk overload.

Technically, the DS proxy is implemented as an [actor service](#actor-service) launched by [node warden](#node-warden) on each node for each storage group, handling all requests to the group (writing, reading, and deleting [LogoBlob](#logoblob), group locking). When writing data, the DS proxy performs [erasure coding](#erasure-coding) of the data, splitting the LogoBlob into parts that are then sent to the corresponding VDisks. The DS proxy performs the reverse process when reading, receiving parts from VDisks and reconstructing the LogoBlob from them.

#### Node warden {#node-warden}

**Node warden** or `BS_NODE` is an [actor service](#actor-service) on each cluster node that launches [PDisks](#pdisk), [VDisks](#vdisk), and [DS proxies](#ds-proxy) of [static storage groups](#static-group) when the node starts. Also, it interacts with the [DS controller](#ds-controller) to launch PDisk, VDisk, and DS proxies of [dynamic groups](#dynamic-group). DS proxies of dynamic groups are launched on demand: node warden processes "undelivered" messages to DS proxies, launches the corresponding DS proxies, and receives the group configuration from the DS controller.

#### Failure realm {#fail-realm}

**Failure realm** or **fail realm** is a set of [failure domains](#fail-domain) that can fail simultaneously due to a common cause. A correlated failure of two [VDisks](#vdisk) in the same failure realm is more likely than a failure of two VDisk from different failure realms.

An example of a failure realm is a set of equipment located in a single [data center, or availability zone](#regions-az), which can fail entirely due to a natural disaster, large-scale power outage, or other similar event.

#### Failure domain {#fail-domain}

**Failure domain** or **fail domain** is a set of equipment that can fail simultaneously. A correlated failure of two [VDisk](#vdisk) within a single failure domain is more likely than a failure of two VDisks from different failure domains. In the case of different failure domains, the probability of simultaneous failure also depends on whether the domains in question belong to the same failure realm or different ones.

An example of a failure domain is a set of disks connected to a single server, since all disks of a particular server may become unavailable if the server's power supply or network controller fails. Typically, all servers located in a single [server rack](#rack) are considered a common failure domain, because power or network issues at the rack level cause all equipment in it to become unavailable. Thus, a typical failure domain corresponds to a server rack (if the [cluster](#cluster) is configured with rack-aware topology) or a single server.

Failure domain-level failures are automatically handled by {{ ydb-short-name }} without stopping the cluster.

#### Distributed storage channel {#channel}

**Distributed storage channel**, **channel**, **distributed storage channel**, **DS channel**, or **channel** is a logical connection between a [tablet](#tablet) and a [distributed storage](#distributed-storage) group. A tablet can write data to various channels, and each channel maps to a specific [storage group](#storage-group). Having multiple channels allows a tablet to:

* Write more data than a single storage group can contain.
* Store different [LogoBlob](#logoblob) in different storage groups, with different properties, such as erasure coding or on different media (HDD, SSD, NVMe).

### Distributed transaction implementation {#transaction-implementation}

Below are explained the terms related to the implementation of [distributed transactions](#transactions).{% if oss == true %} The implementation itself is described in a separate article [{#T}](../contributor/datashard-distributed-txs.md).{% endif %}

#### Deterministic transactions {#deterministic-transactions}

Distributed transactions in {{ ydb-short-name }} are inspired by the research paper [Building Deterministic Transaction Processing Systems without Deterministic Thread Scheduling](http://cs-www.cs.yale.edu/homes/dna/papers/transactions-wodet11.pdf) by Alexander Thomson and Daniel J. Abadi from Yale University. The paper introduces the concept of **deterministic transaction processing**, which allows efficient processing of distributed transactions. The original paper imposed restrictions on the types of operations that can be performed this way. Since these restrictions hindered real user scenarios, {{ ydb-short-name }} evolved its algorithms to execute them, using deterministic transactions as stages of user transaction execution with additional orchestration and locking.

#### Optimistic locking {#optimistic-locking}

As in many other database management systems, queries in {{ ydb-short-name }} can place locks on certain data fragments, such as table rows, to ensure that concurrent changes do not lead to an inconsistent state. However, {{ ydb-short-name }} checks these locks not at the beginning of transactions, but when attempting to commit them. The first approach is called **pessimistic locking** (for example, used in PostgreSQL), and the second is called **optimistic locking** (used in {{ ydb-short-name }}).

#### Transaction lock invalidation {#tli}

**Transaction Lock Invalidation (TLI)** is the normal behavior of {{ ydb-short-name }} when parallel transactions conflict within [optimistic locks](#optimistic-locking). If one transaction (the violator) writes data and thereby breaks the locks of another transaction (the victim), {{ ydb-short-name }} detects this when the victim commits and rolls it back with error `transaction locks invalidated`. For more details on TLI diagnostics, see [{#T}](../troubleshooting/performance/queries/transaction-lock-invalidation.md).

#### Preparation phase {#prepare-stage}

**Preparation phase** is the transaction phase during which the transaction body is registered on all participating shards.

#### Execution phase {#execute-stage}

**Execution phase** is the transaction phase during which the planned transaction is executed and a response is generated.

In some cases, instead of [preparation](#prepare-stage) and execution, the transaction is executed immediately and a response is generated. For example, this happens for transactions that affect only one shard or for consistent reads from a data snapshot.

#### Dirty operations {#dirty-operations}

In the case of read-only transactions, similar to "read uncommitted" in other database management systems, reading data that has not yet been committed to disk may be required. This is called **dirty operations**.

#### Read-write set {#rw-set}

**Read-write set**, **RW set**, or **RW-set** is a set of data that will participate in the execution of a [distributed transaction](#transactions). It combines the read set data that will be read and the write set for which modifications will be performed.

#### Read set {#read-set}

**Read set**, **ReadSet data**, or **read set** is what participating shards send during transaction execution. In the case of data transactions, it may contain information about the state of [optimistic locks](#optimistic-locking), shard readiness to commit, or a decision to abort the transaction.

#### Transaction proxies {#transaction-proxy}

**Transaction proxies**, **transaction proxy**, or `TX_PROXY` is a service that orchestrates the execution of many [distributed transactions](#transactions): sequential phases, phase execution, planning, and result aggregation. In the case of direct orchestration by other actors (for example, QP data transactions), it is used for caching and allocating unique [TxIDs](#txid).

#### Transaction flags {#txflags}

**Transaction flags**, **transaction flags**, or **TxFlags** is a bitmask of flags that somehow modify the execution of a transaction.

#### Transaction ID {#txid}

**Transaction ID** or **TxID** is a unique identifier assigned to each transaction when it is accepted by {{ ydb-short-name }}.

#### Transaction order ID {#transaction-order-id}

**Transaction order ID** or **transaction order id** is a unique identifier assigned to each transaction during planning. It consists of [PlanStep](#planstep) and [Transaction ID](#txid).

#### Plan step {#planstep}

**Plan step**, **step**, **PlanStep**, or **Step** is the logical time at which the execution of a set of transactions is scheduled.

#### Mediator time {#mediator-time}

During the execution of distributed transactions, **mediator time** is the logical time up to which (inclusive) a participating shard must know the entire execution plan. It is used to advance time in the absence of transactions on a particular shard, to determine whether it can read from a snapshot.

#### MiniKQL {#minikql}

**MiniKQL** is a language that allows expressing a single [deterministic transaction](#deterministic-transactions) in the system. It is a functional, strictly typed language. Conceptually, the language describes a graph of reading from the database, performing computations on the read data, and writing results to the database and/or to a special document representing the query result (for display to the user). A MiniKQL transaction must explicitly specify its read set (data to be read) and assume deterministic branching (for example, no randomness).

MiniKQL is a low-level language. End users of the system only see queries in [YQL](#yql), which relies on MiniKQL in its implementation.

#### Query Processor {#kqp}

**Query Processor** or **QP** (formerly **KQP**) is a {{ ydb-short-name }} component responsible for orchestrating the execution of user queries and generating the final response.

### Global schema {#global-schema}

**Global schema**, **database schema**, **global scheme**, **global schema** or **database schema** is the schema of all data stored in the [database](#database). It consists of [tables](#table) and other entities such as [topics](#topic). The metadata about these entities is called the global schema. The term is used in contrast to **local schema**, which refers to the data schema inside a [tablet](#tablet). {{ ydb-short-name }} users never see the local schema and work only with the global schema.

### KiKiMR {#kikimr}

**KiKiMR** is the outdated name of {{ ydb-short-name }}, used before it became an [open source product](https://github.com/ydb-platform/ydb). It can still be found in source code, old articles and videos, etc.
