# Glossary {{ ydb-short-name }}

This article provides an overview of the terms and definitions used in {{ ydb-short-name }} and its documentation. It [starts with key terms](#key-terminology) that are useful to become familiar with early in working with {{ ydb-short-name }}, and the rest of the article contains [more advanced terms](#advanced-terminology) that may be useful later.

## Key terminology {#key-terminology}

This section describes terms that are useful to anyone working with {{ ydb-short-name }}, regardless of their role and use case.

### Cluster {#cluster}

**cluster** {{ ydb-short-name }} is a set of interrelated [nodes](#node) {{ ydb-short-name }} that exchange data to execute user queries and reliably store data. These nodes form one of the supported [cluster topologies](#topology), which directly affects its reliability and performance characteristics.

Clusters {{ ydb-short-name }} are multi-tenant (multi-user) and can contain multiple isolated [databases](#database).

### Database {#database}

As in most database management systems, a **database** in {{ ydb-short-name }} is a logical container for other entities such as [tables](#table). However, in {{ ydb-short-name }} the namespace within databases is hierarchical, like in [virtual file systems](https://en.wikipedia.org/wiki/Virtual_file_system), and thus [directories](#folder) allow more structured organization of entities.

Another important characteristic of {{ ydb-short-name }} databases is that they are typically allocated separate compute resources. As a result, creating a database requires additional actions by [DevOps engineers](../devops/index.md).

### Node {#node}

{{ ydb-short-name }} **node** is a server process that runs the executable named `ydbd`. Several nodes {{ ydb-short-name }} can run on a single physical server or virtual machine, which is common practice. Thus, in the context of {{ ydb-short-name }}, nodes **are not** synonyms for hosts.

Since {{ ydb-short-name }} uses a storage and compute separation approach, `ydbd` has several operating modes that define the node type. The available node types are described below.

#### Database node {#database-node}

**Database nodes** (also known as **tenant nodes**, **compute nodes**) process user queries addressed to a specific logical [database](#database). Their state resides only in memory and can be restored from [distributed storage](#distributed-storage). The set of database nodes of a given [cluster {{ ydb-short-name }}](topology.md) can be considered the compute layer of that cluster. Thus, adding database nodes and allocating them additional resources (CPU and RAM) are the primary ways to increase the compute resources of a database.

The primary role of database nodes is to run various [tablets](#tablet) and [actors](#actor), as well as to accept incoming network requests.

#### Storage node {#storage-node}

**Storage nodes** are stateful nodes responsible for long‑term storage of data fragments. The set of storage nodes of a given [cluster {{ ydb-short-name }}](#cluster) is called [distributed storage](#distributed-storage) and can be considered the storage layer of that cluster. Thus, adding additional storage nodes and their disks are the primary ways to increase storage capacity and the cluster’s I/O throughput.

#### Hybrid node {#hybrid-mode}

**Hybrid node** is a process that simultaneously performs both the roles of a [database node](#database-node) and a [storage node](#storage-node). Hybrid nodes are often used for development purposes. For example, you can run a container with a full‑featured {{ ydb-short-name }} containing only a single `ydbd` process in hybrid mode. In production environments they are used rarely.

#### Static node {#static-node}

**Static nodes** are manually configured during the initial initialization or reconfiguration of a cluster. Typically, they serve as [storage nodes](#storage-node), but it is technically possible to configure them also as [database nodes](#database-node).

#### Dynamic node {#dynamic}

**dynamic nodes** are added and removed from the cluster on the fly. They can serve only as [database nodes](#database-node).

### Distributed storage {#distributed-storage}

**Distributed storage**, **Blob storage**, or **BlobStorage** is a distributed fault‑tolerant data storage layer in {{ ydb-short-name }}. It provides a specialized API intended for storing immutable data fragments of [tablet](#tablet).

A number of terms related to the [implementation of distributed storage](#distributed-storage-implementation) are discussed below.

### Storage group {#storage-group}

**storage group** is a location for reliable data storage, similar to [RAID](https://en.wikipedia.org/wiki/RAID), but using disks of multiple servers. Depending on the chosen [cluster topology](#topology), storage groups employ different algorithms to ensure high availability, similar to [standard RAID levels](https://en.wikipedia.org/wiki/Standard_RAID_levels).

[Distributed storage](#distributed-storage) typically manages a large number of relatively small storage groups. Each group can be assigned to a specific [database](#database) to increase the disk space capacity and I/O throughput available to that database.

[Static](#static-group) and [dynamic](#dynamic-group) storage groups are physical, meaning their data is placed directly on [VDisk](#vdisk).

#### Static group {#static-group}

**static group** is a special [storage group](#storage-group) created during the initial deployment of the cluster. Its primary role is to store data of system [tablets](#tablet), which can be considered as metadata for the entire cluster level.

A static group may require special attention during major cluster maintenance, such as decommissioning an [availability zone](#regions-az).

#### Dynamic group {#dynamic-group}

Regular storage groups that are not [static](#static-group) are called **dynamic groups** or **dynamic group**. They are called dynamic because they can be created and deleted on the fly during the operation of the [cluster](#cluster).

#### Virtual storage group {#virtual-storage-groups}

**virtual storage group** is an entity that is not actually a [storage group](#storage-group), but appears as one from the outside (provides a similar external interface). It can store its data in other storage groups or in S3.

### Storage pool {#storage-pool}

**storage pool** is a set of data storage devices with similar characteristics. Each storage pool is assigned a unique name within the cluster {{ ydb-short-name }}. Technically, each storage pool consists of many physical disks ( [PDisk](#pdisk)). Each [storage group](#storage-group) is created in a specific storage pool, which determines the performance characteristics of the storage group by selecting appropriate storage devices. Typically, separate storage pools are created for devices of different types (e.g., NVMe, SSD, and HDD) or for specific models of those devices that have varying capacity and access speed.

### Actor {#actor}

[Actor model](https://en.wikipedia.org/wiki/Actor_model) is one of the primary approaches to execution parallelism used in {{ ydb-short-name }}. In this model **actors** or **actor** are lightweight processes in user space that can have and modify their private state, but can affect each other only indirectly via message passing. {{ ydb-short-name }} has its own implementation of this model, which is described [below](#actor-implementation).

In {{ ydb-short-name }}, actors with reliably persisted state are called [tablets](#tablet).

### Tablet {#tablet}

**tablet** is one of the core building blocks and abstractions of {{ ydb-short-name }}. It represents an entity responsible for a relatively small segment of user or system data. Typically, a tablet manages data volumes up to several gigabytes, though some tablet types can handle larger volumes.

For example, a [row user table](#row-oriented-table) is managed by one or more tablets of type [DataShard](#data-shard), each tablet being responsible for a continuous range of [primary keys](#primary-key) and their associated data.

End users sending queries to the {{ ydb-short-name }} cluster for execution do not need to know the details of tablets, their types, or how they work, although this can be useful, for example, for performance optimization.

Technically, tablets are [actors](#actor) with state reliably stored in [distributed storage](#distributed-storage). This state allows a tablet to continue operating on another [database node](#database-node) if the previous one fails or becomes overloaded.

[Details of tablet implementation](#tablet-implementation) and related terms, as well as [primary tablet types](#tablet-types), are discussed below.

### Transactions {#transactions}

{{ ydb-short-name }} implements **transactions** on two main levels:

* [Local database](#local-database) and the rest of the [tablet infrastructure](#tablet-implementation) allow [tablets](#tablet) to manipulate their state using **local transactions** with a [serializable isolation level](https://en.wikipedia.org/wiki/Isolation_(database_systems)#Serializable). Technically they are not local to a single node, because this state is stored remotely in [distributed storage](#distributed-storage).
* In the context of {{ ydb-short-name }}, the term **distributed transactions** usually refers to transactions that span multiple tablets. For example, transactions between tables or even rows of a single table are often distributed.
* **Single-shard** transactions span a single tablet and execute faster. For example, transactions between rows of a single table partition are often single-shard.

These mechanisms enable {{ ydb-short-name }} to provide [strict consistency](https://en.wikipedia.org/wiki/Consistency_model#Strict_consistency).

{% if oss %}

The implementation of distributed transactions is covered in a separate article [{#T}](../contributor/datashard-distributed-txs.md), and below is a list of several [related terms](#deterministic-transactions).

{% endif %}

### Sessions

Logical connections to the database that store the context required for executing queries and managing transactions. Sessions are described in more detail in section [{#T}](query_execution/index.md#sessions).

### Client-side timeout {#client-timeout}

**client-side timeout** — a time limit that an application or {{ ydb-short-name }} SDK waits for a database operation to complete (for example, query execution or receiving a gRPC response). When this time expires, the client typically aborts the wait: it closes the connection or data stream, receives a transport or SDK error—before the server has returned an explicit response (see [codes](../reference/ydb-sdk/ydb-status-codes.md) of the server response {{ ydb-short-name }}).

If the client-side timeout is shorter than the query execution time on the {{ ydb-short-name }} side, then due to the cluster’s request handling characteristics, a client‑aborted request may continue to run on the server for some time. If this situation occurs at scale, the server becomes overloaded with requests whose responses the client is not waiting for. Consequently, frequent retries of the same request immediately after a timeout can exacerbate the overload. See the articles [{#T}](../troubleshooting/performance/queries/retry-cascade.md) and [{#T}](../troubleshooting/performance/queries/overloaded-errors.md) for more details; retry policies in the SDK are described in section [{#T}](../reference/ydb-sdk/error_handling.md).

### Implicit transactions {#implicit-transactions}

**Implicit transaction** is a query execution mode in which the [transaction mode](transactions.md#modes) is not specified. In this case, {{ ydb-short-name }} determines on its own whether to wrap them in a transaction. This mode is described in more detail in [{#T}](transactions.md#implicit).

### Multi-version concurrency control {#mvcc}

[**multi-version concurrency control**](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) or **MVCC** — a method used by {{ ydb-short-name }} for concurrent access by multiple parallel transactions to the database without mutual interference. It is described in more detail in a separate article [{#T}](query_execution/mvcc.md).

### Streaming queries {#streaming-query}

A query type designed for [stream processing](https://en.wikipedia.org/wiki/Stream_processing) of an unbounded data stream. Unlike regular queries, streaming queries have no execution time limits, automatically restart on errors, and periodically persist their state as [checkpoints](#streaming-queries-checkpoints) to ensure fault tolerance.

Streaming queries are described in more detail in a separate article [{#T}](streaming-query.md).

### Streaming query checkpoints {#streaming-queries-checkpoints}

Periodically saved state of a [streaming query](#streaming-query) required for automatic recovery of its operation after failures in a distributed system. More about checkpoints in the article [{#T}](../dev/streaming-query/checkpoints.md).

### Topology {#topology}

{{ ydb-short-name }} supports multiple **topologies** of a [cluster](#cluster) (or **topology**), described in more detail in a separate article [{#T}](topology.md). Below are several related terms explained.

#### Availability zones and regions {#regions-az}

**Availability zone** is a data center or its isolated segment with minimal physical distance between nodes and minimal risk of failure simultaneously with other availability zones. Thus, availability zones should not share common infrastructure such as power, cooling, or external network connections.

**Region** is a large geographic area containing multiple availability zones. The distance between availability zones in the same region should be about 500 km or less. {{ ydb-short-name }} writes data to each availability zone in a region synchronously, providing reasonable latency and uninterrupted operation in case one availability zone fails.

#### Rack {#rack}

**Rack**, **server rack** is equipment used to organize placement of multiple servers. Servers in the same rack are more likely to become unavailable simultaneously due to rack-level issues such as power, cooling, etc. {{ ydb-short-name }} can take into account which server is in which rack when placing each data fragment in environments based on physical servers.

#### Pile {#pile}

**Pile** is a set of nodes that can fail or be shut down simultaneously while preserving the operability of other parts of the cluster (pile). A pile can maintain functionality when other cluster nodes are disconnected. Piles are used in [bridge mode](#bridge) to split the cluster into several parts between which synchronous replication occurs. A pile can consist of nodes from one or multiple regions.

#### Bridge mode {#bridge}

**Bridge mode** is a special cluster topology in which data is stored with synchronous replication between multiple [piles](#pile). The mode's features are described in [{#T}](topology.md#bridge) and also in [{#T}](bridge.md).

### Table {#table}

**Table** — a structured fragment of information organized into rows and columns. Each row represents a single record or item, and each column is a specific attribute or field with a defined data type.

There are two main approaches to representing tabular data in memory or on disk: [row-oriented (row by row)](#row-oriented-table) and [column-oriented (column by column)](#column-oriented-table). The chosen approach strongly affects performance characteristics of operations on this data: the former is better suited for transactional workloads (OLTP), while the latter is for analytical (OLAP). {{ ydb-short-name }} supports both approaches.

#### Row-oriented table {#row-oriented-table}

**Row-oriented tables** store data for all or most columns of each row physically next to each other. They are described in more detail in [{#T}](datamodel/table.md#row-oriented-tables).

#### Column-oriented table {#column-oriented-table}

**Column-oriented tables**, **columnar table** store data for each column separately. They are optimized for building aggregates over a small number of columns, but are less suitable for accessing specific rows, as rows must be reconstructed from their cells on the fly. They are described in more detail in [{#T}](datamodel/table.md#column-oriented-tables).

#### Primary key {#primary-key}

**Primary key** — an ordered list of columns whose values uniquely identify a row. It is used to create a [primary index](#primary-index) for the table. It is defined by the user {{ ydb-short-name }} when [creating a table](../yql/reference/syntax/create_table/index.md) and significantly impacts the performance of operations on that table.

A guide to choosing primary keys is presented in [{#T}](../dev/primary-key/index.md).

#### Primary index {#primary-index}

**primary index**, **primary key index** — is the primary data structure used to locate rows in a table. It is created based on the selected [primary key](#primary-key) and defines the physical order of rows in the table; thus, each table can have only one primary index. The primary index is unique.

#### Secondary index {#secondary-index}

**secondary index** — is an additional data structure used to locate rows in a table, typically when this cannot be done efficiently using the [primary index](#primary-index). Unlike a primary index, secondary indexes are managed independently of the table's main data. Thus, a table can have multiple secondary indexes for different scenarios. The capabilities {{ ydb-short-name }} regarding secondary indexes are described in a separate article [{#T}](query_execution/secondary_indexes.md). A secondary index can be either unique or non‑unique.

Special types of secondary indexes are highlighted separately — [vector index](#vector-index), [full-text index](#fulltext-index), and [JSON index](#json-index).

#### Vector index {#vector-index}

**vector index** — is an additional data structure used to speed up solving the task of [vector search](query_execution/vector_search.md) when the data volume is large and [exact vector search without an index](../yql/reference/udf/list/knn.md) does not work satisfactorily. The capabilities {{ ydb-short-name }} for approximate nearest neighbor (ANN) search using vector indexes are described in a separate article [{#T}](../dev/vector-indexes.md).

**vector index** — is a specialized type of [secondary index](#secondary-index) designed for similarity-based search, unlike traditional secondary indexes that are optimized for equality or range searches.

#### Full-text index {#fulltext-index}

**full-text index** — is an additional data structure used to accelerate text search on a table column (by words and phrases, and when using n-grams — also by substrings).

The full-text search capabilities and index parameters are described in the articles [{#T}](../dev/fulltext-indexes.md) and [{#T}](query_execution/fulltext_search.md).

#### JSON index {#json-index}

**JSON index** — is an auxiliary data structure used to accelerate predicates with the functions [JSON_EXISTS](../yql/reference/builtins/json.md#json_exists) and [JSON_VALUE](../yql/reference/builtins/json.md#json_value) on a column of type `Json` or `JsonDocument`. Unlike traditional secondary indexes, which are optimized for equality or range searches on individual table columns, the JSON index works with arbitrary [JsonPath](../yql/reference/builtins/json.md#jsonpath) paths inside a JSON document.

JSON index, like the [full-text index](#fulltext-index), is built on top of the [inverted index](https://en.wikipedia.org/wiki/Inverted_index), but uses its own tokenizer for JSON documents. The JSON search capabilities are described in the articles [{#T}](../dev/json-indexes.md) and [{#T}](query_execution/json_search.md).

#### Local index {#local-index}

A local index is an auxiliary structure that is stored together with the table data (as opposed to the [global secondary index](#secondary-index), which materializes a separate index table). A local index is used when reading the primary table on the storage side. More details: [local indexes](query_execution/local_indexes.md).

#### Bloom filter {#bloom-filter}

A Bloom filter is a [probabilistic data structure](https://en.wikipedia.org/wiki/Bloom_filter) that allows you to quickly test whether an element belongs to a set. False positives are possible, but false negatives are not.

#### Local Bloom index {#local-bloom-skip-index}

Local Bloom index — a special case of [local index](#local-index): a probabilistic filter on column values based on the [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter), accelerating selective queries by skipping data fragments where the sought value is guaranteed to be absent. More details: [Bloom indexes](../dev/bloom-skip-indexes.md), [local indexes](query_execution/local_indexes.md).

#### Column family {#column-family}

**column family** or **column group** — this is a feature that allows storing subsets of columns of a [row table](#row-oriented-table) separately in a distinct family or group. The primary use case is storing a portion of columns on different types of disks (moving less important columns to HDD) or with different compression settings. If your workload requires many column families, consider using [columnar tables](#column-oriented-table).

#### Column encoding {#column-encoding}

**column encoding** — this is a mechanism for optimizing storage of data in table columns that reduces disk space usage and speeds up certain operations.

#### Time to live {#ttl}

**time to live**, or **TTL** — this is a mechanism for automatically deleting old rows from a table asynchronously in the background. It is described in a separate article [{#T}](ttl.md).

### View {#view}

**view** — this is a way to save a query and treat its results as a real table. The view itself stores no data except the query text. The query stored in the view is executed on each SELECT from it, producing the returned result. Any changes to the tables referenced by the view are immediately reflected in the read results from it.

{% if feature_view %}

Views can be user-defined or system.

#### User-defined views {#user-view}

**user-defined views** are created by the user using the command [{#T}](../yql/reference/syntax/create-view.md). They are described in more detail in [{#T}](../concepts/datamodel/view.md).

{% endif %}

#### System views {#system-view}

**system views** — these are special views automatically created by the system to monitor the state of the database and the cluster. They reside in the special directory `.sys` located in the root directory of each database. System views for databases are described in [{#T}](../dev/system-views.md); system views for the cluster, as well as access‑control issues, are in [{#T}](../devops/observability/system-views.md).

### Topic {#topic}

**message queue** is used for reliable asynchronous communication between various systems via message passing. {{ ydb-short-name }} provides infrastructure that ensures "exactly once" semantics in such communications. Using it, you can achieve guarantees of no lost messages and no accidental duplicates.

**topic** — this is a named entity in a message queue intended for interaction between [producers](#producer) and [consumers](#consumer).

Several terms related to topics are listed below. How topics work {{ ydb-short-name }} is explained in more detail in a separate article [{#T}](datamodel/topic.md).

#### Partition {#partition}

For horizontal scaling, topics are divided into separate elements called **partitions**. Thus, partitions are the unit of parallelism within a topic. Messages within each partition are ordered.

However, data subsets managed by a single [data shard](#data-shard) or [column shard](#column-shard) can also be called partitions.

#### Offset {#offset}

**offset** — this is a sequential number that identifies a message within a [partition](#partition).

#### Producer {#producer}

**producer** — this is an entity that writes new messages to a topic.

#### Consumer {#consumer}

**consumer** — this is an entity that reads messages from a topic.

### Change data capture {#cdc}

**change data capture**, **CDC** — this is a mechanism that allows subscribing to a **change stream** in a specific [table](#table). Technically it is implemented on top of [topics](#topic). It is described in more detail in a separate article [{#T}](cdc.md).

#### Change stream {#changefeed}

**change stream** — an ordered list of changes to a [table](#table) placed in a [topic](#topic).

### Backup collection {#backup-collection}

**Backup collection** — is a [schema object](#scheme-object) that organizes full and incremental [backups](#backup) for selected [row tables](#row-oriented-table). Collections provide [point-in-time recovery](https://en.wikipedia.org/wiki/Point-in-time_recovery), maintaining [backup chains](#backup-chain) and guaranteeing consistent restoration of multiple tables. A table can belong to only one backup collection at a time.

See more details [{#T}](datamodel/backup-collection.md).

#### Backup {#backup}

**Backup** — a copy of data at a specific point in time that can be used to restore data. In the context of [backup collections](#backup-collection) there are two types:

- **Full backup**: A complete snapshot of all data in the collection. It serves as the basis for [backup chains](#backup-chain) and can be restored independently.
- **Incremental backup**: Captures only changes (inserts, updates, deletions) since the previous backup. Requires the entire backup chain for restoration.

#### Backup chain {#backup-chain}

**Backup chain** — an ordered sequence of [backups](#backup) that starts with a full backup, followed by zero or more incremental backups. Each incremental backup depends on all previous backups in the chain. Deleting any backup in the chain makes subsequent incremental backups unrecoverable.

{% if feature_async_replication == true %}

### Asynchronous replication instance {#async-replication-instance}

**Asynchronous replication instance** — a named entity that stores settings for [asynchronous replication](async-replication.md) (connection settings, list of replicated objects, etc.). It can also be used to obtain information about the state of asynchronous replication: [initial scan progress](async-replication.md#initial-scan), [lag](async-replication.md#replication-of-changes), [errors](async-replication.md#error-handling), and so on.

#### Replicated object {#replicated-object}

**Replicated object** — an object (e.g., a table) for which asynchronous replication is configured.

#### Replica object {#replica-object}

**Replica object** — a “mirror copy” of a replicated object, automatically created by an asynchronous replication instance. It is typically read‑only.

{% endif %}

{% if feature_transfer == true %}

### Transfer instance {#transfer-instance}

**Transfer instance** — a named entity that stores settings for [transfer](transfer.md), including connection settings and data transformation rules. It can also be used to obtain information about the transfer state, for example [errors](transfer.md#error-handling).

{% endif %}

### Coordination node {#coordination-node}

**Coordination node** — a schema object that allows client applications to create semaphores for coordinating their actions. Coordination nodes are used for implementing distributed locks, service discovery, leader election, and other scenarios. More details on [coordination nodes](./datamodel/coordination-node.md).

#### Semaphore {#semaphore}

**Semaphore** — an object inside a [coordination node](#coordination-node) that provides a synchronization mechanism for distributed applications. Semaphores can be permanent or temporary and support create, acquire, release, and monitor operations. See more about [semaphores in {{ ydb-short-name }}](./datamodel/coordination-node.md#semaphore).

{% if feature_resource_pool == true and feature_resource_pool_classifier == true %}

### Resource pool {#resource-pool}

**Resource pool** — a schema object that describes limits applied to resources (CPU, RAM, etc.) available for executing queries in this resource pool. A query always runs in some resource pool. By default, all queries run in the resource pool named `default`, which imposes no limits. More details on using resource pools can be found in the article [{#T}](../dev/resource-consumption-management.md).

### Resource pool classifier {#resource-pool-classifier}

**Resource pool classifier** — an object designed to manage the distribution of queries among [resource pools](#resource-pool). It defines rules by which a resource pool is selected for each query. These classifiers are global for the entire [database](#database) and apply to all queries sent to it. More details on their usage can be found in the article [{#T}](../dev/resource-consumption-management.md).

{% endif %}

### YQL {#yql}

**YQL ({{ ydb-short-name }} Query Language)** — is a high-level language for working with the system. It is a dialect of [ANSI SQL](https://en.wikipedia.org/wiki/SQL). There are many resources about YQL, including a [tutorial](../dev/yql-tutorial/index.md), a [reference](../yql/reference/syntax/index.md) and [recipes](../yql/reference/recipes/index.md).

### Federated queries {#federated-queries}

**federated queries** — is a functionality that allows you to query data stored in systems external to the {{ ydb-short-name }} cluster.

Below are several terms related to federated queries. How federated queries work in {{ ydb-short-name }} is explained in more detail in a separate article [{#T}](query_execution/federated_query/index.md).

#### External data source {#external-data-source}

**external data source** or **external connection** — is metadata describing how to connect to a supported external system for executing [federated queries](#federated-queries).

#### External table {#external-table}

**external table** — is metadata describing a specific data set that can be extracted from an [external data source](#external-data-source).

#### Secret {#secret}

**secret** — is confidential metadata that requires special handling. For example, secrets can be used in definitions of [external data sources](#external-data-source) and represent entities such as passwords and tokens.

### Authentication token {#auth-token}

**auth token** — a token used for [authentication](../security/authentication.md) in {{ ydb-short-name }}.

{{ ydb-short-name }} supports [different types of authentication](../security/authentication.md) and various token types.

### Cluster schema {#scheme}

**cluster schema {{ ydb-short-name }}** — is a hierarchical namespace of the {{ ydb-short-name }} cluster. The top-level element of this namespace is the [cluster schema root](#scheme-root). Child elements of the cluster schema root are [databases](#database). Inside each database you can create an arbitrary hierarchy of [objects](#scheme-object) (tables, topics, etc.) using nested directories.

### Database schema {#scheme-database}

**database schema** — is a subset of the cluster's hierarchical namespace that belongs to a database.

### Database root {#scheme-database-root}

**database root** — is the path to a database in the cluster schema.

### Schema root {#scheme-root}

**cluster schema root** — is the root element of the [{{ ydb-short-name }} namespace](datamodel/cluster-namespace.md), whose child elements are [databases](#database).

### Schema object {#scheme-object}

The database schema consists of **schema objects**, which can be databases, [tables](#table) (including [external tables](#external-table)), [topics](#topic), [folders](#folder), etc.

For organizational convenience, schema objects form a hierarchy using [folders](#folder).

### Folder {#folder}

As in file systems, a **folder** or **directory** is a container for [schema objects](#scheme-object).

Folders can contain subfolders, and such nesting can be of arbitrary depth.

### Access object {#access-object}

**access object** during [authorization](../security/authorization.md) — an entity for which access rights and restrictions are configured. In {{ ydb-short-name }}, access objects are [schema objects](#scheme-object).

Each [schema object](#scheme-object) has an [owner](#access-owner) and a [list of rights](#access-control-list) on that object, granted to users and groups ([access subjects](#access-subject)).

### Access subject {#access-subject}

**access subject** — an entity that can access [access objects](#access-object) and perform certain actions in the system.

Access during these calls and actions depends on the configured [list of rights](#access-control-list) and the subject's [access level](#access-level).

An access subject can be a [user](#access-user) or a [group](#access-group).

### Access right {#access-right}

**[access right](../security/authorization.md#right)** — an entity that reflects permission for an [access subject](#access-subject) to perform a specific set of operations in a cluster or database on a particular [access object](#access-object).

### Access rights inheritance {#access-right-inheritance}

**Access rights inheritance** – a mechanism whereby [access rights](#access-right) granted on parent [access objects](#access-object) are inherited by child objects in the hierarchical database structure. This ensures that permissions granted at a higher hierarchy level apply to all lower levels unless they are [explicitly overridden](../reference/ydb-cli/commands/scheme-permissions.md#clear-inheritance).

### Access rights list {#access-control-list}

**[Access rights list](../security/authorization.md#right)**, **access control list**, or **ACL** – a list of all [rights](#access-right) granted to [access subjects](#access-subject) (users and groups) for a specific [access object](#access-object).

### Access level {#access-level}

**Access level** provides the [access subject](#access-subject) with additional capabilities when working with [schema objects](#scheme-object), as well as the ability to perform operations on the cluster as a whole. {{ ydb-short-name }} uses hierarchical access levels:

- Database
- Viewer
- Monitoring
- Administration

An access level for a subject is configured using [access level lists](#access-level-list).

### Access level list {#access-level-list}

**Access level list** or **permissions list** – a list of [SID](#access-sid)s of [access subjects](#access-subject) that are granted a particular [access level](#access-level).

In {{ ydb-short-name }} there are [several such lists](../reference/configuration/security_config.md#security-access-levels) that determine who has which [access levels](#access-level).

Detailed information about access level lists, their hierarchy, and operation principles is provided in the [Access level lists](../security/authorization.md#access-level-lists) section of the authorization documentation.

### Owner {#access-owner}

**[Owner](../security/authorization.md#owner)** – a [subject](#access-subject) ([user](#access-user) or [group](#access-group)) that has full rights on a specific [access object](#access-object).

### User {#access-user}

**[User](../security/authorization.md#user)** – a person who uses {{ ydb-short-name }} to perform a specific function.

In {{ ydb-short-name }} there are different types of users depending on how they are created:

- Local users in {{ ydb-short-name }} databases.
- External users from external directories.

A user is identified by [SID](#access-sid).

#### Local user {#local-user}

A user whose account is created directly in {{ ydb-short-name }} using the YQL command `CREATE USER` or during [initial security configuration](../security/builtin-security.md).

#### External user {#external-user}

A {{ ydb-short-name }} user whose account is created in an external directory, such as an LDAP directory or an IAM system.

### Group {#access-group}

**[Group](../security/authorization.md#group)** or **access group** – a named set of [users](#access-user) and other groups with equal privileges for its members.

A group is identified by [SID](#access-sid).

### Role {#access-role}

A role is a named set of [access rights](#access-right) used to assign to [users](#access-user) or [groups](#access-group).

Roles in {{ ydb-short-name }} are implemented using [groups](#access-group) that are created during the initial cluster deployment and are assigned a specific [permissions list](#access-right) at the root of the cluster schema. For more details on roles, see the article [{#T}](../security/builtin-security.md).

### SID {#access-sid}

**SID** or **security identifier** – a string of the form `<name>` or `<name>@<auth-domain>` that identifies a [subject](../concepts/glossary.md#access-subject). It is used for [authentication](../security/authentication.md), [authorization](../security/authorization.md), in [access rights lists](#access-control-list) and in [access level lists](#access-level-list).

SID identifies an individual [user](#access-user) or [user group](#access-group).

An optional suffix `@<auth-domain>` identifies the source of the subject, i.e., an external directory or system from which it was obtained. For example, users or groups from an LDAP directory may have the suffix `@ldap`. The absence of a suffix means that the user or group is created and exists directly in {{ ydb-short-name }}.

### Query optimizer {#optimizer}

[**Query optimizer**](https://en.wikipedia.org/wiki/Query_optimization) – a set of {{ ydb-short-name }} components responsible for transforming the logical representation of a query into a concrete physically executable plan to obtain the requested result. The main goal of the optimizer is to choose, among all possible query execution plans, one that is sufficiently efficient in terms of predicted execution time and cluster resource consumption. It is described in more detail in a separate article [{#T}](query_execution/optimizer.md).

### Compilation cache {#compile-cache}

**compile cache** — a cache of compiled queries on each [node](#node) of the cluster. It is used to avoid recompilation: if the query text is already in the node's cache, additional compilation is not performed. See the [Query compile cache](../dev/system-views.md#compile-cache-queries) section for more details.

## Advanced terminology {#advanced-terminology}

This section explains terms that are useful for [{{ ydb-short-name }} contributors](../contributor/index.md) and users who want to gain a deeper understanding of what happens inside the system.

### Actor implementation {#actor-implementation}

#### Actor system {#actor-system}

**actor system** — a C++ library with [implementation](https://github.com/ydb-platform/ydb/tree/main/ydb/library/actors) of the [actor model](https://en.wikipedia.org/wiki/Actor_model) for the needs of {{ ydb-short-name }}.

#### Actor service {#actor-service}

**actor service** — an [actor](#actor) that has a well-known name and typically runs as a single instance on a [node](#node).

#### ActorId {#actorid}

**ActorId** — a unique identifier of an actor or a [tablet](#tablet) in a [cluster](#cluster).

#### Actor system interconnect {#actor-system-interconnect}

**actor system interconnect**, **interconnect** — is the internal network layer of the [cluster](#cluster). All [actors](#actor) communicate with each other in the system via the interconnect.

#### Local {#local}

**local** — a [actor service](#actor-service) that runs on every [node](#node). It directly manages [tablets](#tablet) on its node and interacts with [Hive](#hive). It registers with Hive and receives commands to launch tablets.

### Tablet implementation {#tablet-implementation}

[**tablet**](#tablet) — a [actor](#actor) with persistent state. It includes a set of data that the tablet is responsible for and a finite state machine through which the tablet's data (or state) changes. The tablet is a fault‑tolerant entity because its data are stored in [distributed storage](#distributed-storage) that survives disk and node failures. The tablet automatically restarts on another [node](#node) in case of failure or overload of the previous one. Data in the tablet change sequentially, as the system infrastructure guarantees that there is no more than one [tablet leader](#tablet-leader) through which tablet data changes are applied.

A tablet solves the same problem as the [Paxos](https://en.wikipedia.org/wiki/Paxos_(computer_science)) and [Raft](https://en.wikipedia.org/wiki/Raft_(algorithm)) algorithms in other systems, namely the [distributed consensus](https://en.wikipedia.org/wiki/Consensus_(computer_science)) problem. From a technical perspective, a tablet's implementation can be described as a Replicated State Machine (RSM) built on top of a shared log, because the tablet's state is fully represented by an ordered command log stored in distributed, fault‑tolerant storage.

During execution, the tablet's state machine is managed by three components:

1. The common tablet part ensures log consistency and recovery in case of failures.
2. **executor** — an abstraction of a local database, i.e., the data structures and code that organize work with the data stored by the tablet.
3. An actor with user code that implements the specific logic of a particular tablet type.

In {{ ydb-short-name }} there are several types of specialized tablets that store all kinds of data for various tasks. Many {{ ydb-short-name }} features, such as [tables](#table) and [topics](#topic), are implemented as different tablet types. Thus, reusing the tablet infrastructure is one of the key mechanisms for extending {{ ydb-short-name }} as a platform.

Typically, a {{ ydb-short-name }} cluster runs orders of magnitude more tablets than the processes or threads that other systems would use for a cluster of comparable size. In a {{ ydb-short-name }} cluster, hundreds of thousands to millions of tablets can operate simultaneously.

Since a tablet stores its state in [distributed storage](#distributed-storage), it can be (re)started on any cluster node. Tablets are identified by a [TabletID](#tabletid), a 64‑bit number assigned when the tablet is created.

### Tablet leader {#tablet-leader}

**tablet leader** is the current active leader of that tablet. The tablet leader receives commands, assigns them an order, and confirms them to the outside world. It is guaranteed that at any moment there is at most one leader for each tablet.

### Tablet candidate {#tablet-candidate}

**tablet candidate** is one of the election participants that wants to become the [leader](#tablet-leader) of that tablet. If the candidate wins the election, it becomes the tablet leader.

### Tablet replica {#tablet-follower}

**tablet replica**, **tablet follower**, or **hot standby** is a copy of the [tablet leader](#tablet-leader) that applies the command log accepted by the leader (with some delay). A tablet may have zero or more replicas. Replicas perform two main functions:

* In case of leader termination or failure, replicas are the preferred [candidates](#tablet-candidate) for the role of new leader, because they can become leader much faster than other candidates, having applied most of the log.
* Replicas can serve read‑only queries if the client explicitly selects the optional relaxed transaction mode that allows stale reads.

### Tablet generation {#tablet-generation}

**tablet generation** is the number that identifies the reincarnation of a tablet leader. It changes only when a new leader is elected and always increases.

### Tablet local database {#local-database}

**tablet local database**, **local database** — is a set of data structures and associated code that manage the state of a tablet and its stored data. Logically, the state of the local database is represented by a set of tables very similar to relational tables. Modification of the local database state is performed by tablet local transactions created by the tablet’s user actor.

Each table of the local database is stored as an [LSM tree](#lsm-tree).

#### Log-structured merge-tree {#lsm-tree}

**[Log-structured merge-tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree)** is a data structure designed to optimize write and read performance in storage systems. It is used in {{ ydb-short-name }} to store tables of the [local database](#local-database) and data of [VDisks](#vdisk).

#### MemTable {#memtable}

All data written to tables of the [local database](#local-database) are initially stored in an in‑memory data structure called **MemTable**. When a MemTable reaches the configured size, it is flushed to disk as an immutable data structure [SST](#sst).

#### Sorted string table {#sst}

**Sorted string table**, **SST** — is an immutable data structure that stores table rows sorted by key, facilitating efficient key lookup and range queries. Each SST consists of a continuous series of small data pages, typically about 7 KiB each, which further optimizes disk read performance. An SST is usually part of an [LSM tree](#lsm-tree).

#### Tablet pipe {#tablet-pipe}

**tablet pipe**, **TabletPipe** — is a virtual connection that can be established with a tablet. It includes locating the [tablet leader](#tablet-leader) by [TabletID](#tabletid). This is the recommended way to work with a tablet. The term **open tablet pipe** describes the process of resolving (finding) a tablet in the cluster and establishing a virtual communication channel with it.

#### TabletID {#tabletid}

**TabletID** is the unique identifier of a [tablet](#tablet) within a cluster.

#### Bootstrapper {#bootstrapper}

**Bootstrapper** is the primary mechanism for launching tablets, used for system tablets (for example, for [Hive](#hive), [DS controller](#ds-controller), the root [SchemeShard](#scheme-shard)). [Hive](#hive) initializes the other tablets.

### Shared cache {#shared-cache}

**shared cache** is an [actor](#actor) that stores data pages recently read from the [distributed storage](#distributed-storage). Caching these pages reduces disk I/O operations and speeds up data retrieval, improving overall system performance.

### Memory controller {#memory-controller}

**memory controller**— is a [actor](#actor) that manages [memory limits](../reference/configuration/memory_controller_config.md) {{ ydb-short-name }}.

### Spilling {#spilling}

**spilling** — is a memory management mechanism in {{ ydb-short-name }} that temporarily offloads intermediate query data to external storage when such data exceeds the available RAM of a node. In {{ ydb-short-name }}, disk is currently used for spilling.

For more details about spilling, see [{#T}](query_execution/spilling.md).

### Tablet types {#tablet-types}

[Tablets](#tablet) can be viewed as a framework for building reliable components that run in a distributed system. Many components {{ ydb-short-name }}—both system components and those that handle user data—are implemented using this framework; the main ones are listed below.

#### SchemeShard {#scheme-shard}

**SchemeShard**, **Scheme shard** — is a system tablet that stores the database schema, including metadata of user [tables](#table), [topics](#topic), etc.

In addition, there is a **root SchemeShard** that stores information about databases created in the cluster.

#### DataShard {#data-shard}

**DataShard**, **Data shard** — is a tablet that manages a segment of a [row-oriented user table](datamodel/table.md#row-oriented-tables). A logical user table is divided into segments by continuous ranges of the table’s primary key. Each such range is handled by a separate DataShard tablet. The range itself is also called a [partition](#partition). The DataShard tablet stores data row‑by‑row, which is efficient for OLTP workloads.

#### ColumnShard {#column-shard}

**ColumnShard**, **Column shard** — is a tablet that stores a data segment of a [columnar user table](datamodel/table.md#column-oriented-tables).

#### KeyValue Tablet {#kv-tablet}

**KeyValue**, **KV Tablet**, or **key‑value tablet** — is a tablet that implements a simple key → value mapping, where keys and values are strings. It also provides several specific features, such as locks.

#### PersQueue Tablet {#pq-tablet}

**PersQueue**, or **persistent queue tablet** — is a tablet that implements the concept of a [topic](#topic). Each topic consists of one or more partitions, and each partition is managed by a separate instance of a PQ tablet.

#### TxAllocator {#txallocator}

**TxAllocator**, or **transaction allocator** — is a system tablet that allocates unique transaction identifiers ( [TxID](#txid)) in the cluster. Typically, the cluster has several such tablets, from which the [transaction proxy](#transaction-proxy) pre‑allocates and caches ranges for local issuance within a single process.

#### Coordinator {#coordinator}

**coordinator** — is a system tablet that provides global transaction ordering. The coordinator’s task is to assign logical time [PlanStep](#planstep) to each transaction planned through that coordinator. Each transaction is assigned exactly one coordinator, selected by hashing its [TxId](#txid).

#### Mediator {#mediator}

**mediator** — is a system tablet that distributes transactions planned by [coordinators](#coordinator) among transaction participants. Mediators ensure the advancement of global time. Each transaction participant is associated with exactly one mediator. Mediators eliminate the need for a full mesh of connections between all coordinators and all participants of all transactions.

#### Hive {#hive}

**Hive** — is a system tablet responsible for launching and managing other tablets. Its duties include moving tablets between nodes in case of node failure or overload [node](#node).{% if audience != "corp" %} For more information about Hive, see the [separate article](../contributor/hive.md).{% endif %}

#### CMS {#cms}

**CMS**, or **cluster management system** — is a system tablet responsible for managing information about the current state of the [cluster {{ ydb-short-name }}](#cluster). This information is used to perform gradual cluster restarts without affecting user workloads, maintenance, cluster reconfiguration, etc.

#### NodeBroker {#node-broker}

**NodeBroker** — is a system tablet that handles registration of [dynamic nodes](#dynamic) in the cluster.

#### BSController {#ds-controller}

**BSController**, **blob storage controller** manages the dynamic configuration of the distributed storage, including information about [PDisk](#pdisk), [VDisk](#vdisk) and [storage groups](#storage-group). It interacts with [node warden](#node-warden) to launch various distributed storage components. It interacts with [Hive](#hive) to allocate [channels](#channel) to tablets.

#### Console {#console}

**Console** is a system tablet responsible for storing the [dynamic configuration](../devops/configuration-management/configuration-v1/dynamic-config.md) and delivering it to cluster nodes.

#### Kesus {#kesus}

**Kesus** is a tablet that implements a [coordination node](datamodel/coordination-node.md).

#### SysViewProcessor {#sys-view-processor}

**SysViewProcessor** is a tablet that stores data from some [system views](../dev/system-views.md).

{% if feature_serial %}

#### SequenceShard {#sequence-shard}

**SequenceShard** is a tablet that serves Sequence objects used to implement [serial data types](../yql/reference/types/serial.md).

{% endif %}

{% if feature_async_replication %}

#### ReplicationController {#replication-controller}

**ReplicationController** is a tablet responsible for the [asynchronous replication](async-replication.md) process.

{% endif %}

#### StatisticsAggregator {#statistics-aggregator}

**StatisticsAggregator** is a tablet responsible for collecting statistics used in cost optimization.

### Slot {#slot}

**Slot** in {{ ydb-short-name }} can be used in two contexts:

* **Slot** is a portion of server resources allocated for running a single [node](#node) {{ ydb-short-name }}. A typical slot size is 10 CPU cores and 50 GB of RAM. Slots are used when a cluster {{ ydb-short-name }} is deployed on servers or virtual machines with enough resources to host multiple slots.
* **VDisk slot** or **VSlot** is a portion of a [PDisk](#pdisk) that can be allocated to one of the [VDisk](#vdisk).

### State storage {#state-storage}

**State storage**, **state storage**, **StateStorage** is a distributed service that stores information about tablets, namely:

* The current tablet leader or its absence.
* Tablet replicas.
* The tablet’s generation and step `(generation:step)`.

State storage is used as a service for resolving tablet names, i.e., for obtaining an [ActorId](#actorid) from a [TabletID](#tabletid). StateStorage is also used in the tablet leader selection process ([tablet leader](#tablet-leader)).

The information in state storage is volatile. Consequently, it is lost on power loss or process restart. Despite its name, this service is not a permanent long‑term storage. It holds only information that is easy to recover and does not need to be durable. However, state storage keeps data on multiple nodes to minimize the impact of node failures. Through this service you can also gather a quorum used for tablet leader selection.

Because of its nature, the state storage service operates on a best‑effort basis. For example, the absence of multiple tablet leaders is guaranteed by the leader‑selection protocol on the [distributed storage](#distributed-storage), not by state storage.

### Board {#board}

**Board** is a distributed service designed to store metadata as key‑value pairs. It is used, among other things, to store information about [endpoints](../concepts/connect.md#endpoint).

### SchemeBoard {#scheme-board}

**SchemeBoard** is a distributed service designed to store metadata as key‑value pairs. It is used, among other things, to store information about [schemas](#global-schema).

#### Compaction {#compaction}

**Compaction**, **compaction** is an internal background process that reorganizes data of an [LSM tree](#lsm-tree). Data in [VDisk](#vdisk) and [local databases](#local-database) are organized as LSM trees. Therefore, there is **VDisk compaction** and **tablet compaction**. The compaction process is usually quite resource‑intensive, so measures are taken to minimize its overhead, for example by limiting the number of concurrent compactions.

#### gRPC proxy {#grpc-proxy}

**gRPC proxy** is a proxy system for external client requests. Client requests arrive in the system via the [gRPC](https://grpc.io) protocol, then the proxy component translates them into internal calls to execute those requests, which are passed through the [interconnect](#actor-system-interconnect). This proxy provides an interface for both request‑response and bidirectional streaming.

### Distributed configuration {#distributed-configuration}

**Distributed configuration**, **DistConf** is an internal mechanism for [configuration](../devops/configuration-management/configuration-v2/config-overview.md) of a cluster that provides startup and setup of [static nodes](#static-node), automatic management of a [static storage group](#static-group) and [State Storage](../concepts/glossary.md#state-storage). Distributed configuration starts before any [tablets](#tablet), [storage groups](#storage-group) and [State Storage](../concepts/glossary.md#state-storage) begin.

More details about the design of distributed configuration are described in [{#T}](../contributor/configuration-v2.md).

### Implementation of distributed storage {#distributed-storage-implementation}

**Distributed storage** is a distributed fault‑tolerant data storage layer that stores binary records called [LogoBlob](#logoblob), addressed using a specific identifier type called [LogoBlobID](#logoblobid). Thus, distributed storage is a key‑value store that maps a LogoBlobID to a string up to 10 MiB in size. Distributed storage consists of many [storage groups](#storage-group), each of which is an independent data repository.

**Distributed storage** stores immutable data, with each immutable data block identified by a specific LogoBlobID key. The API of distributed storage is highly specific and intended only for use by [tablets](#tablet) to store their data and change logs. Consequently, it is not meant for general‑purpose data storage. Data in distributed storage are deleted using special barrier commands. Because its interface lacks mutations, distributed storage can be implemented without implementing [distributed consensus](https://en.wikipedia.org/wiki/Consensus_(computer_science)). Distributed storage is just one of the components that tablets use to implement distributed consensus.

#### LogoBlob {#logoblob}

**LogoBlob** is a set of binary immutable data identified by [LogoBlobID](#logoblobid) and stored in [distributed storage](#distributed-storage). The data block size is limited at the [VDisk](#vdisk) level and above in the stack. Currently, the maximum data block size that VDisk can handle is 10 MiB.

#### LogoBlobID {#logoblobid}

**LogoBlobID** is an identifier of a [LogoBlob](#logoblob) in [distributed storage](#distributed-storage). It has the structure shown in `[TabletID, Generation, Step, Channel, Cookie, BlobSize, PartID]`. The main components of a LogoBlobID are:

* `TabletID` — the [ID](#tabletid) of the tablet to which the LogoBlob belongs.
* `Generation` — the generation of the tablet in which the data block was written.
* `Channel` — the [channel](#channel) of the tablet on which the LogoBlob is written.
* `Step` — an incremental counter, typically within the tablet's generation.
* `Cookie` — a unique identifier of a data block within a single `Step`. A cookie is usually used when writing multiple data blocks into one `Step`.
* `BlobSize` — the size of the LogoBlob.
* `PartID` — the identifier of a part of a data block. It is important when the original LogoBlob is split into parts using [error‑correcting coding](#erasure-coding), and the parts are written to the corresponding [VDisk](#vdisk) and [storage groups](#storage-group).

#### Replication {#replication}

**Replication** is a process that ensures a sufficient number of data copies (replicas) to maintain the desired availability characteristics of the cluster {{ ydb-short-name }}. It is typically used in geo‑distributed clusters {{ ydb-short-name }}.

#### Error‑correcting coding {#erasure-coding}

[**erasure coding**](https://en.wikipedia.org/wiki/Erasure_code) is a data encoding method in which the original data is supplemented with redundancy and split into multiple fragments, providing the ability to reconstruct the original data if one or more fragments are lost. It is widely used in clusters {{ ydb-short-name }} with a single [availability zone](#regions-az) unlike [replication](#replication) with three replicas. For example, the most popular 4+2 erasure coding scheme offers the same reliability as three replicas, with a storage overhead of 1.5 versus 3.

#### PDisk {#pdisk}

**PDisk**, **physical disk** — is a component that controls a physical disk storage (block device). In other words, PDisk is a subsystem that implements an abstraction similar to a specialized file system on top of block devices (or files emulating a block device for testing purposes). PDisk provides data integrity checks (including [erasure coding](#erasure-coding) of sector groups for reconstructing data on individual damaged sectors, integrity verification via checksums), transparent encryption of all data on the disk, and transactional guarantees for disk operations (write confirmation strictly after `fsync`).

PDisk contains a scheduler that shares the device’s bandwidth among multiple clients ( [VDisk](#vdisk)). PDisk divides the block device into blocks called [slots](#slot) (about 128 MiB in size; smaller blocks are also allowed). At any given time, no more than one VDisk can own a given slot. PDisk also maintains a recovery log shared by the PDisk service and all VDisks.

#### VDisk {#vdisk}

**VDisk**, **virtual disk** — is a component that implements storage of [distributed storage](#distributed-storage) [LogoBlob](#logoblob) on [PDisk](#pdisk). VDisk stores all its data on PDisk. One VDisk corresponds to one PDisk, but typically several VDisks are associated with a single PDisk. Unlike PDisk, which hides blocks and logs, VDisk provides an interface at the LogoBlob level and [LogoBlobID](#logoblobid), for example writing a LogoBlob, reading data via LogoBlobID, and deleting a set of LogoBlobs using a special command. VDisk is a member of a [storage group](#storage-group). An individual VDisk is local, but many VDisks in the group ensure reliable data storage. VDisks in the group synchronize data with each other and replicate data in case of loss. The set of VDisks in a storage group forms a distributed RAID.

#### Yard {#yard}

**Yard** is the name of the [PDisk](#pdisk) API. It allows [VDisk](#vdisk) to read and write data to blocks and logs, reserve blocks, delete blocks, and transactionally acquire and release block ownership. In some contexts, Yard can be considered a synonym for PDisk.

#### Skeleton {#skeleton}

**Skeleton** is an [actor](#actor) that provides an interface to [VDisk](#vdisk).

#### SkeletonFront {#skeletonfront}

**SkeletonFront** is a proxy actor for Skeleton that controls the flow of messages arriving at Skeleton.

#### Proxy {#ds-proxy}

**distributed storage proxy**, **DS-proxy**, or **BS-proxy** serves as a client library for performing operations on [distributed storage](#distributed-storage). The users of the DS-proxy are [tablets](#tablet) that write to and read from the distributed storage. The DS-proxy hides the distributed nature of the storage from the user. The DS-proxy’s task is to write to a quorum of [VDisk](#vdisk), perform retries when necessary, and control the write/read flow to prevent overloading VDisk.

Technically, the DS proxy is implemented as an [actor service](#actor-service), launched by the [node warden](#node-warden) on each node for each storage group, handling all requests to the group (writes, reads, and deletions of [LogoBlob](#logoblob), group locking). When writing data, the DS proxy performs [error-correcting encoding](#erasure-coding) of the data, splitting the LogoBlob into parts that are then sent to the corresponding VDisk. The DS proxy performs the reverse process when reading, receiving parts from VDisk and reconstructing the LogoBlob from them.

#### Node warden {#node-warden}

**Node warden** or `BS_NODE` — is a [actor service](#actor-service) on each cluster node, launching [PDisks](#pdisk), [VDisks](#vdisk) and [DS proxy](#ds-proxy) [static storage groups](#static-group) when the node starts. It also interacts with the [DS controller](#ds-controller) to launch PDisk, VDisk and DS proxy [dynamic groups](#dynamic-group). The DS proxy for dynamic groups is launched on demand: node warden processes “undelivered” messages to the DS proxy, launches the appropriate DS proxies and obtains the group configuration from the DS controller.

#### Failure realm {#fail-realm}

**failure realm** or **fail realm** is a set of [failure domains](#fail-domain) that can fail simultaneously for a common cause. A correlated failure of two [VDisks](#vdisk) in the same failure realm is more likely than a failure of two VDisks from different failure realms.

An example of a failure realm is a set of equipment located in a single [data center or availability zone](#regions-az) that can fail entirely due to a natural disaster, a large‑scale power outage, or a similar event.

#### Failure domain {#fail-domain}

**failure domain** or **fail domain** is a set of equipment that can fail simultaneously. A correlated failure of two [VDisk](#vdisk) within the same failure domain is more likely than a failure of two VDisk from different failure domains. When the failure domains are different, the probability of simultaneous failure also depends on whether the considered domains belong to the same failure realm or to different ones.

An example of a failure domain is a set of disks attached to a single server, because all disks of that server can become unavailable if the server’s power supply or network controller fails. Typically, a common failure domain includes all servers located in the same [server rack](#rack), since power or network issues at the rack level render all equipment in the rack unavailable. Thus, a typical failure domain corresponds to a server rack (if the [cluster](#cluster) is configured with rack‑aware topology) or to an individual server.

Failure domain‑level failures are automatically handled by {{ ydb-short-name }} without stopping the cluster.

#### Distributed storage channel {#channel}

**distributed storage channel**, **DS channel**, or **channel** is a logical connection between a [tablet](#tablet) and a [distributed storage](#distributed-storage) group. A tablet can write data to multiple channels, and each channel maps to a specific [storage group](#storage-group). Having several channels allows a tablet to:

* Write more data than a single storage group can hold.
* Store different [LogoBlob](#logoblob) in different storage groups, with varying properties such as error‑correcting coding or on different media (HDD, SSD, NVMe).

### Implementation of distributed transactions {#transaction-implementation}

The terms related to the implementation of [distributed transactions](#transactions) are explained below.{% if oss == true %} The implementation itself is described in a separate article [{#T}](../contributor/datashard-distributed-txs.md).{% endif %}

#### Deterministic transactions {#deterministic-transactions}

Distributed transactions {{ ydb-short-name }} are inspired by the research paper [Building Deterministic Transaction Processing Systems without Deterministic Thread Scheduling](http://cs-www.cs.yale.edu/homes/dna/papers/transactions-wodet11.pdf) by Alexander Thomson and Daniel J. Abadi of Yale University. The paper introduced the concept of **deterministic transaction processing**, which enables efficient handling of distributed transactions. The original paper imposed restrictions on the types of operations that could be performed in this way. Because these restrictions hindered real‑world user scenarios, {{ ydb-short-name }} developed its own algorithms to execute them, using deterministic transactions as stages of user transaction execution with additional orchestration and locking.

#### Optimistic locking {#optimistic-locking}

As in many other database management systems, queries {{ ydb-short-name }} can place locks on specific data fragments, such as table rows, to ensure that concurrent modifications do not leave them in an inconsistent state. However, {{ ydb-short-name }} checks these locks not at the beginning of transactions but at commit time. The first approach is called **pessimistic locking** or **perssimistic locking** (for example, used in PostgreSQL), and the second is **optimistic locking** (used in {{ ydb-short-name }}).

#### Transaction lock invalidation {#tli}

**Transaction Lock Invalidation** (Transaction Lock Invalidation, **TLI**) — this is the normal behavior {{ ydb-short-name }} when parallel transactions conflict under [optimistic locking](#optimistic-locking). If one transaction (the violator) writes data and thereby breaks the locks of another transaction (the victim), {{ ydb-short-name }} detects this at the victim’s commit and aborts it with error `transaction locks invalidated`. For more on diagnosing TLI, see [{#T}](../troubleshooting/performance/queries/transaction-lock-invalidation.md).

#### Preparation phase {#prepare-stage}

**Preparation phase** — this is the transaction phase during which the transaction body is registered on all participating shards.

#### Execution phase {#execute-stage}

**Execution phase** — this is the transaction phase during which the planned transaction is executed and a response is generated.

In some cases, instead of [preparation](#prepare-stage) and execution, a transaction is executed immediately and a response is generated. For example, this occurs for transactions that involve only a single shard or for consistent reads from a data snapshot.

#### Dirty operations {#dirty-operations}

For read‑only transactions, similar to “read uncommitted” in other database management systems, it may be necessary to read data that has not yet been persisted to disk. This is called **dirty operations**.

#### Read‑write set {#rw-set}

**read-write set**, **RW-set** — this is the set of data that will participate in the execution of a [distributed transaction](#transactions). It combines the read‑set data that will be read and the write set for which modifications will be applied.

#### Read set {#read-set}

**read set**, **ReadSet data** — this is what participating shards transmit during transaction execution. For data‑bearing transactions it may contain information about the state of [optimistic locks](#optimistic-locking), the shard’s readiness to commit, or a decision to abort the transaction.

#### Transaction proxies {#transaction-proxy}

**transaction proxies**, **transaction proxy**, or `TX_PROXY` — this is a service that orchestrates the execution of many [distributed transactions](#transactions): sequential phases, phase execution, planning, and result aggregation. When directly orchestrated by other actors (e.g., QP data transactions), it is used for caching and allocating unique [TxID](#txid).

#### Transaction flags {#txflags}

**transaction flags**, **TxFlags** — this is a bitmask of flags that somehow modify transaction execution.

#### Transaction identifier {#txid}

**Transaction identifier**, **TxID** — this is a unique identifier assigned to each transaction when it is accepted {{ ydb-short-name }}.

#### Transaction order identifier {#transaction-order-id}

**Transaction order identifier**, **transaction order id** — this is a unique identifier assigned to each transaction during planning. It consists of a [PlanStep](#planstep) and a [Transaction ID](#txid).

#### Plan step {#planstep}

**Plan step**, **PlanStep**, **Step** — this is the logical time at which the execution of a set of transactions is scheduled.

#### Mediator time {#mediator-time}

During the execution of distributed transactions, **Mediator time**, **mediator time** — this is the logical time up to which (inclusive) a participant shard must know the entire execution plan. It is used to advance time when there are no transactions on a particular shard, to determine whether it can read from a snapshot.

#### MiniKQL {#minikql}

**MiniKQL** — is a language that allows expressing a single [deterministic transaction](#deterministic-transactions) in the system. It is a functional, strongly typed language. Conceptually, the language describes a read graph from the database, the execution of computations over the read data, and the writing of results back to the database and/or to a special document representing the query result (for display to the user). A MiniKQL transaction must explicitly specify its read set (the data to be read) and assume a deterministic choice of execution branches (for example, no randomness).

**MiniKQL** is a low-level language. End users of the system see only queries in the [**YQL**](#yql) language, which relies on **MiniKQL** in its implementation.

#### Query Processor {#kqp}

**Query Processor**, **QP**, (formerly **KQP**) is a {{ ydb-short-name }} component responsible for orchestrating the execution of user queries and generating the final response.

### Global schema {#global-schema}

**global scheme**, **global schema**, or **database schema** is the schema of all data stored in the [database](#database). It consists of [tables](#table) and other entities such as [topics](#topic). Metadata about these entities is called the global schema. The term is used in contrast to **local schema**, which refers to the data schema inside a [tablet](#tablet). {{ ydb-short-name }} users never see the local schema and work only with the global schema.

### KiKiMR {#kikimr}

**KiKiMR** is a legacy name for {{ ydb-short-name }} that was used before it became an [open‑source product](https://github.com/ydb-platform/ydb). It may still appear in source code, old articles, videos, etc.
