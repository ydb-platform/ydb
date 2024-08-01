# {{ ydb-short-name }} glossary

This article is an overview of terms and definitions used in {{ ydb-short-name }} and its documentation. It [starts with key terms](#key-terminology) that will be useful to get acquainted with early when you start working with {{ ydb-short-name }}, while the rest of it is more [advanced](#advanced-terminology) and might be helpful later on.

## Key terminology {#key-terminology}

This section explains terms that are useful to any person working with {{ ydb-short-name }} regardless of their role and use case.

### Cluster {#cluster}

A {{ ydb-short-name }} **cluster** is a set of interconnected {{ ydb-short-name }} [nodes](#node) that communicate with each other to serve user queries and reliably store user data. These nodes form one of the supported [cluster topologies](#topology), which directly affects the cluster's reliability and performance characteristics.

{{ ydb-short-name }} clusters are multitenant and can contain multiple isolated [databases](#database).

### Database {#database}

Like in most database management systems, a **database** in {{ ydb-short-name }} is a logical container for other entities like [tables](#table). However, in {{ ydb-short-name }}, the namespace inside databases is hierarchical like in [virtual file systems](https://en.wikipedia.org/wiki/Virtual_file_system), and thus [folders](#folder) allow for further organization of entities.

Another essential characteristic of {{ ydb-short-name }} databases is that they typically have dedicated compute resources allocated to them. Hence, creating an additional database is usually done externally by [DevOps engineers](../devops/index.md) or automation rather than via a SQL query.

### Node {#node}

A {{ ydb-short-name }} **node** is a server process running an executable called `ydbd`. A physical server or virtual machine can run multiple {{ ydb-short-name }} nodes, which is common. Thus, in the context of {{ ydb-short-name }}, nodes are **not** synonymous with hosts.

Given {{ ydb-short-name }} follows the approach of separated storage and compute layers, `ydbd` has multiple operation modes that determine the node type. The available node types are explained below.

#### Database node {#database-node}

**Database nodes** (also known as **tenant nodes**) serve user queries addressed to a specific logical [database](#database). Their state is only in memory and can be recovered from the [Distributed Storage](#distributed-storage). All database nodes of a given [{{ ydb-short-name }} cluster](cluster/index.md) can be considered its compute layer. Thus, adding database nodes and allocating extra CPU and RAM to them are the main ways to increase the database's compute resources.

The main role of database nodes is to run various [tablets](#tablet) and [actors](#actor), as well as accept incoming requests via various endpoints.

#### Storage node {#storage-node}

**Storage nodes** are stateful and responsible for long-term persisting pieces of data. All storage nodes of a given [{{ ydb-short-name }} cluster](#cluster) are called [Distributed Storage](#distributed-storage) and can be considered the cluster's storage layer. Thus, adding extra storage nodes and their disks are the main ways to increase the cluster's storage capacity and input/output throughput.

#### Hybrid node {#hybrid-mode}

A **hybrid node** is a process that simultaneously serves both roles of a [database](#database-node) and [storage](#storage-node) node. Hybrid nodes are often used for development purposes. For instance, you can run a container with a full-featured {{ ydb-short-name }} containing only one process, `ydbd`, in hybrid mode. They are rarely used in production environments.

#### Static node {#static-node}

**Static nodes** are manually configured during the initial cluster initialization or re-configuration. Typically, they play the role of [storage nodes](#storage-node), but technically, it is possible to configure them to be [database nodes](#database-node) as well.

#### Dynamic node {#dynamic-node}

**Dynamic nodes** are added and removed from the cluster on the fly. They can only play the role of [database nodes](#database-node).

### Distributed storage {#distributed-storage}

**Distributed storage**, **Blob storage**, or **BlobStorage** is a distributed fault-tolerant data persistence layer of {{ ydb-short-name }}. It has a specialized API designed for storing immutable pieces of [tablet's](#tablet) data.

Multiple terms related to the [distributed storage implementation](#distributed-storage-implementation) are covered below.

### Storage group {#storage-group}

A **storage group**, **Distributed storage group**, or **Blob storage group** is a location for reliable data storage similar to [RAID](https://en.wikipedia.org/wiki/RAID), but using disks of multiple servers. Depending on the chosen [cluster topology](#topology), storage groups use different algorithms to ensure high availability, similar to [standard RAID levels](https://en.wikipedia.org/wiki/Standard_RAID_levels).

[Distributed storage](#distributed-storage) typically manages a large number of relatively small storage groups. Each group can be assigned to a specific [database](#database) to increase disk capacity and input/output throughput available to this database.

#### Static group {#static-group}

A **static group** is a special [storage group](#storage-group) created during the initial cluster deployment. Its primary role is to store system [tablet's](#tablet) data, which can be considered cluster-wide metadata.

A static group might require special attention during major maintenance, such as decommissioning an [availability zone](#regions-az).

#### Dynamic group {#dynamic-group}

Regular storage groups that are not [static](#static-group) are called **dynamic groups**. They are called dynamic because they can be created and decommissioned on the fly during [cluster](#cluster) operation.

### Storage pool {#storage-pool}

**Storage pool** is a collection of data storage devices with similar characteristics. Each storage pool is assigned a unique name within a {{ ydb-short-name }} cluster. Technically each storage pool consisis of multiple [PDisks](#pdisk). Each [storage group](#storage-group) is created in the particular storage pool, which determines the performance characteristics of the storage group through the selection of the appropriate storage devices. It is typical to have separate storage pools for NVMe, SSD and HDD devices, or for the models of those devices having different capacity and speed.

### Actor {#actor}

The [actor model](https://en.wikipedia.org/wiki/Actor_model) is one of the main approaches for concurrent programming, which is employed by {{ ydb-short-name }}. In this model, **actors** are lightweight user-space processes that may have and modify their private state but can only affect each other indirectly through message passing. {{ ydb-short-name }} has its own implementation of this model, which is covered [below](#actor-implementation).

In {{ ydb-short-name }}, actors with the reliably persisted state are called [tablets](#tablet).

### Tablet {#tablet}

A **tablet** is one of {{ ydb-short-name }}'s primary building blocks and abstractions. It is an entity responsible for a relatively small segment of user or system data. Typically, a tablet manages up to single-digit gigabytes of data, but some kinds of tablets can handle more.

For example, a [row-oriented user table](#row-oriented-table) is managed by one or more [DataShard](#datashard) tablets, with each tablet responsible for a continuous range of [primary keys](#primary-key) and the corresponding data.

End users sending queries to a {{ ydb-short-name }} cluster aren't expected to know much about tablets, their kinds, or how they work, but it might still be helpful, for example, for performance optimizations.

Technically, tablets are [actors](#actors) with a persistent state reliably saved in [Distributed Storage](#distributed-storage). This state allows the tablet to continue operating on a different [database node](#database-node) if the previous one is down or overloaded.

[Tablet implementation details](#tablet-implementation) and related terms, as well as [main tablet types](#tablet-types), are covered below in the advanced section.

### Distributed transactions {#distributed-transaction}

{{ ydb-short-name }} implements **transactions** on two main levels:

* [Local database](#local-database) and the rest of [tablet infrastructure](#tablet-implementation) allow [tablets](#tablet) to manipulate their state using **local transactions** with [serializable isolation level](https://en.wikipedia.org/wiki/Isolation_%28database_systems%29#Serializable). Technically, they aren't really local to a single node as such a state persists remotely in [Distributed Storage](#distributed-storage).
* In the context of {{ ydb-short-name }}, the term **distributed transactions** usually refers to transactions involving multiple tablets. For example, cross-table or even cross-row transactions are often distributed.

Together, these mechanisms allow {{ ydb-short-name }} to provide [strict consistency](https://en.wikipedia.org/wiki/Consistency_model#Strict_consistency).

The implementation of distributed transactions is covered in a separate article [{#T}](../contributor/datashard-distributed-txs.md), while below there's a list of several [related terms](#distributed-transactions-implementation).

### Multi-version concurrency control {#mvcc}

[**Multi-version concurrency control**](https://en.wikipedia.org/wiki/Multiversion_concurrency_control) or **MVCC** is a method {{ ydb-short-name }} used to allow multiple concurrent transactions to access the database simultaneously without interfering with each other. It is described in more detail in a separate article [{#T}](mvcc.md).

### Topology {#topology}

{{ ydb-short-name }} supports several [cluster](#cluster) topologies, described in more detail in a separate article [{#T}](topology.md). A few related terms are explained below.

#### Availability zones and regions {#regions-az}

An **availability zone** is a data center or an isolated segment thereof with minimal physical distance between nodes and minimal risk of failure at the same time as other availability zones. Thus, availability zones are expected not to share any infrastructure like power, cooling, or external network connections.

A **region** is a large geographic area containing multiple availability zones. The distance between availability zones in the same region is expected to be around 500 km or less. {{ ydb-short-name }} performs synchronous data writes to each availability zone in a region, ensuring reasonable latencies and uninterrupted performance if an availability zone fails.

#### Rack {#rack}

A **rack** or **server rack** is a piece of equipment used to mount multiple servers in an organized manner. Servers in the same rack are more likely to become unavailable simultaneously due to rack-wide issues related to electricity, cooling, etc. Thus, {{ ydb-short-name }} can consider information about which server is located in which rack when placing each piece of data in bare-metal environments.

### Table {#table}

A **table** is a structured piece of information arranged in rows and columns. Each row represents a single record or entry, while each column represents a specific attribute or field with a particular data type.

There are two main approaches to representing tabular data in RAM or on disk drives: [row-oriented (row-by-row)](#row-oriented-table) and [column-oriented (column-by-column)](#column-oriented-table). The chosen approach greatly impacts the performance characteristics of operations with this data, with the former more suitable for transaction workloads (OLTP) and the latter for analytical (OLAP). {{ ydb-short-name }} supports both.

#### Row-oriented table {#row-oriented-table}

**Row-oriented tables** store data for all or most columns of a given row physically close to each other. They are explained in more detail in [{#T}](datamodel/table.md#row-oriented-tables).

#### Column-oriented table {#column-oriented-table}

**Column-oriented tables** or **columnar tables** store data for each column independently. They are optimized for building aggregates over a small number of columns but are less suitable for accessing particular rows, as rows need to be reconstructed from their cells on the fly. They are explained in more detail in [{#T}](datamodel/table.md#column-oriented-tables).

#### Primary key {#primary-key}

A **primary key** is an ordered list of columns, the values of which uniquely identify rows. It is used to build the [table's primary index](#primary-index). It is provided by the {{ ydb-short-name }} user during [table creation](../yql/reference/syntax/create_table.md) and dramatically impacts the performance of workloads interacting with that table.

The guidelines on choosing primary keys are provided in [{#T}](../dev/primary-key/index.md).

#### Primary index {#primary-index}

A **primary index** or **primary key index** is the main data structure used to locate rows in a table. It is built based on the chosen [primary key](#primary-key) and determines the physical order of rows in a table; thus, each table can have only one primary index. The primary index is unique.

#### Secondary index {#secondary-index}

A **secondary index** is an additional data structure used to locate rows in a table, typically when it can't be done efficiently using the [primary index](#primary-index). Unlike the primary index, secondary indexes are managed independently from the main table data. Thus, a table might have multiple secondary indexes for different use cases. {{ ydb-short-name }}'s capabilities in terms of secondary indexes are covered in a separate article [{#T}](secondary_indexes.md). Secondary indexes can be either unique or non-unique.

#### Column family {#column-family}

A **column family** or **column group** is a feature that allows storing a subset of [row-oriented table](#row-oriented-table) columns separately in a distinct family or group. The primary use case is to store some columns on different kinds of disk drives (offload less important columns to HDD) or with various compression settings. If the workload requires many column families, consider using [column-oriented tables](#column-oriented-table) instead.

#### Time to live {#ttl}

**Time to live** or **TTL** is a mechanism for automatically removing old rows from a table asynchronously in the background. It is explained in a separate article [{#T}](ttl.md).

### Topic {#topic}

A **topic** is a persistent queue that can be used for reliable asynchronous communications between various systems via message passing. {{ ydb-short-name }} provides the infrastructure to ensure "exactly once" semantics in such communications, which ensures that there are both no lost messages and no accidental duplicates.

Several terms related to topics are listed below. How {{ ydb-short-name }} topics work is explained in more detail in a separate article [{#T}](topic.md).

#### Partition {#partition}

For horizontal scaling purposes, topics are divided into separate elements called **partitions**. Thus, a partition is a unit of parallelism within a topic. Messages inside each partition are ordered.

However, subsets of data managed by a single [data shard](#data-shard) or [column shards](#column-shard) can also be called partitions.

#### Offset {#offset}

An **offset** is a sequence number that identifies a message inside a [partition](#partition).

#### Producer {#producer}

A **producer** is an entity that writes new messages to a topic.

#### Consumer {#consumer}

A **consumer** is an entity that reads messages from a topic.

### Change data capture {#cdc}

**Change data capture** or **CDC** is a mechanism that allows subscribing to a stream of changes to a given [table](#table). Technically, it is implemented on top of [topics](#topic). It is described in more detail in a separate article [{#T}](cdc.md).

### YQL {#yql}

**YQL ({{ ydb-short-name }} Query Language)** is a high-level language for working with the system. It is a dialect of [ANSI SQL](https://en.wikipedia.org/wiki/SQL). There's a lot of content covering YQL, including a [tutorial](../dev/yql-tutorial/index.md), [reference](../yql/reference/syntax/index.md), and [recipes](../recipes/yql/index.md).

### Federated queries {#federated-queries}

**Federated queries** is a feature that allows querying data stored in systems external to the {{ ydb-short-name }} cluster.

A few terms related to federated queries are listed below. How {{ ydb-short-name }} federated queries work is explained in more detail in a separate article [{#T}](federated_query/index.md).

#### External data source {#external-data-source}

An **external data source** or **external connection** is a piece of metadata that describes how to connect to a supported external system for [federated query execution](#federated-queries).

#### External table {#external-table}

An **external table** is a piece of metadata that describes a particular dataset that can be retrieved from an [external data source](#external-data-source).

#### Secret {#secret}

A **secret** is a sensitive piece of metadata that requires special handling. For example, secrets can be used in [external data source](#external-data-source) definitions and represent things like passwords and tokens.

### Folder {#folder}

Like in filesystems, a **folder** or **directory** is a container for other entities. In the case of {{ ydb-short-name }}, these entities can be [tables](#table) (including [external tables](#external-table)), [topics](#topic), other folders, etc.

## Advanced terminology {#advanced-terminology}

This section explains terms that are useful to [{{ ydb-short-name }} contributors](../contributor/index.md) and users who want to get a deeper understanding of what's going on inside the system.

### Actors implementation {#actor-implementation}

#### Actor system {#actor-system}

An **actor system** is a C++ library with {{ ydb-short-name }}'s [implementation](https://github.com/ydb-platform/ydb/tree/main/ydb/library/actors) of the [Actor model](https://en.wikipedia.org/wiki/Actor_model).

#### Actor service {#actor-service}

An **actor service** is an actor that has a well-known name and is usually run in a single instance on a [node](#node).

#### ActorId {#actorid}

An **ActorId** is a unique identifier of the actor or [tablet](#tablet) in the [cluster](#cluster).

#### Actor system interconnect {#actor-system-interconnect}

The **actor system interconnect** or **interconnect** is the [cluster's](#cluster internal network layer. All [actors](#actor) interact with each other within the system via the interconnect.

#### Local {#local}

A **Local** is an [actor service](#actor-service) running on each [node](#node). It directly manages the [tablets](#tablet) on its node and interacts with [Hive](#hive). It registers with Hive and receives commands to launch tablets.

### Tablet implementation {#tablet-implementation}

A [**tablet**](#tablet) is an [actor](#actor) with a persistent state. It includes a set of data for which this tablet is responsible and a finite state machine through which the tablet's data (or state) changes. The tablet is a fault-tolerant entity because tablet data is stored in a [Distributed storage](#distributed-storage) that survives disk and node failures. The tablet is automatically restarted on another [node](#node) if the previous one is down or overloaded. The data in the tablet changes in a consistent manner because the system infrastructure ensures that there is no more than one [tablet leader](#tablet-leader) through which changes to the tablet data are carried out.

The tablet solves the same problem as the [Paxos](https://en.wikipedia.org/wiki/Paxos_(computer_science)) and [Raft](https://en.wikipedia.org/wiki/Raft_(algorithm)) algorithms in other systems, namely the [distributed consensus](https://en.wikipedia.org/wiki/Consensus_(computer_science)) task. From a technical point of view, the tablet implementation can be described as a Replicated State Machine (RSM) over a shared log, as the tablet state is completely described by an ordered command log stored in a distributed and fault-tolerant storage.

During execution, the tablet state machine is managed by three components:

1. The generic tabular part ensures the log's consistency and recovery in case of failures.
2. **Executor** is an abstraction of a local database, namely data structures and code that arrange work with the data stored by the tablet.
3. An [actor](#actor) with a custom code that implements the specific logic of a specific tablet type.

In {{ ydb-short-name }}, there are multiple kinds of specialized tablets storing all kinds of data for all sorts of tasks. Many {{ ydb-short-name }} features like [tables](#table) and [topics](#topic) are implemented as specific tablets. Thus, reusing tablet infrastructure is one of the key means of {{ ydb-short-name }} extensibility as a platform.

Usually, there are orders of magnitude more tablets running in a {{ ydb-short-name }} cluster compared to processes or threads that other systems would use for a similarly sized cluster. There can easily be hundreds of thousands to millions of tablets working simultaneously in a {{ ydb-short-name }} cluster.

Since the tablet stores its state in [Distributed storage](#distributed-storage), it can be (re)started on any node of the cluster. Tablets are identified using [TabletID](#tabletid), a 64-bit number assigned when creating a tablet.

### Tablet leader {#tablet-leader}

A **tablet leader** is the current active leader of a given tablet. The tablet leader accepts commands, assigns them an order, and confirms them to the outside world. It is guaranteed that there is no more than one leader for a given tablet at any moment.

### Tablet candidate {#tablet-candidate}

A **tablet candidate** is one of the election participants who wants to become a [leader](#tablet-leader) for a given tablet. If a candidate wins the election, it assumes the tablet leader role.

### Tablet follower {#tablet-follower}

A **tablet follower** or **hot standby** is a copy of a [tablet leader](#tablet-leader) that applies the log of commands accepted by the leader (with some lag). A tablet can have zero or more followers. Followers serve two primary purposes:

* In case of the leader's termination or failure, followers are the preferred [candidates](#tablet-candidate) to become the new leader because they can become the leader much faster than other candidates since they have applied most of the log.
* Followers can respond to read-only queries if a client explicitly chooses the optional relaxed transaction mode that allows for stale reads.

### Tablet generation {#tablet-generation}

A **tablet generation** is a number identifying the reincarnation of the tablet leader. It changes only when a new leader is chosen and always grows.

### Tablet local database {#local-database}

A **tablet local database** or **local database** is a set of data structures and related code that manages the tablet's state and the data it stores. Logically, the local database state is represented by a set of tables very similar to relational tables. Modification of the state of the local database is performed by local tablet transactions generated by the tablet's user actor.

#### Tablet pipe {#tablet-pipe}

A **Tablet pipe** or **TabletPipe** is a virtual connection that can be established with a tablet. It includes resolving the [tablet leader](#tablet-leader) by [TabletID](#tabletid). It is the recommended way to work with the tablet. The term **open a pipe to a tablet** describes the process of resolving (searching) a tablet in a cluster and establishing a virtual communication channel with it.

#### TabletID {#tabletid}

A **TabletID** is a cluster-wide unique [tablet](#tablet) identifier.

#### Bootstrapper {#bootstrapper}

The **bootstrapper** is the primary mechanism for launching tablets, used for service tablets (for example, for [Hive](#hive), [DS controller](#ds-controller), root [SchemeShard](#scheme-shard)). The [Hive](#hive) tablet initializes the rest of the tablets.

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

A **KV Tablet** or **key-value tablet** is a tablet that implements a simple key->value mapping, where keys and values are strings. It also has a number of specific features, like locks.

#### PQ Tablet {#pq-tablet}

A **PQ Tablet** or **persistent queue tablet** is a tablet that implements the concept of a [topic](#topic). Each topic consists of one or more partitions, and each partition is managed by a separate PQ tablet instance.

#### TxAllocator {#txallocator}

A **TxAllocator** or **transaction allocator** is a system tablet that allocates unique transaction identifiers ([TxID](#txid)) within the cluster. Typically, a cluster has several such tablets, from which [transaction proxy](##transaction-proxy) pre-allocates and caches ranges for local issuance within a single process.

#### Coordinator {#coordinator}

The **Coordinator** is a system tablet that ensures the global ordering of transactions. The coordinator's task is to assign a logical [PlanStep](#planstep) time to each transaction planned through this coordinator. Each transaction is assigned exactly one coordinator, chosen by hashing its [TxId](#txid).

#### Mediator {#mediator}

The **Mediator** is a system tablet that distributes the transactions planned by [coordinators](#coordinator) to the transaction participants (usually, [DataShards](#data-shard)). Mediators ensure the advancement of global time. Each transaction participant is associated with exactly one mediator. Mediators allow to avoid the need for a full mesh of connections between all coordinators and all participants in all transactions.

#### Hive {#hive}

A **Hive** is a system tablet responsible for launching and managing other tablets. Its responsibilities include moving tablets between nodes in case of [node](#node) failure or overload.

#### Cluster management system {#cms}

The **cluster management system** or **CMS** is a system tablet responsible for managing the information about the current [{{ ydb-short-name }} cluster](#cluster) state. This information is used to perform cluster rolling restarts without affecting user workloads, maintenance, cluster re-configuration, etc.

### Slot {#slot}

A **slot** in {{ ydb-short-name }} can be used in two contexts:

* **Slot** is a portion of a server's resources allocated to running a single {{ ydb-short-name }} [node](#node). A common slot size is 10 CPU cores and 50 GB of RAM. Slots are used if a {{ ydb-short-name }} cluster is deployed on servers or virtual machines with sufficient resources to host multiple slots.
* [VDisk](#slot) **slot** or **VSlot** is a fraction of PDisk that can be allocated to one of the [VDisks](#vdisk).

### State storage {#state-storage}

A **State storage** or **StateStorage** is a distributed service that stores information about tablets, namely:

* The current leader of the tablet or its absence.
* Tablet followers.
* Generation and step of the tablet `(generation:step)`.

State storage is used as a name service for resolving tablets, i.e., getting [ActorId](#actorid) by [TabletID](#tabletid). StateStorage is also used in the process of electing the [tablet leader](#tablet-leader).

Information in state storage is volatile. Thus, it is lost when the power is turned off, or the process is restarted. Despite the name, this service is not persistent storage. It contains only information that is easily recoverable and does not have to be durable. However, state storage keeps information on several nodes to minimize the impact of node failures. Through this service, it is possible to gather a quorum, which is used to elect tablet leaders.

Due to its nature, the state storage service operates in a best-effort manner. For example, the absence of several tablet leaders is guaranteed through the leader election protocol on [distributed storage](#distributed-storage), not state storage.

#### Compaction {#compaction}

**Compaction** is the internal background process of rebuilding [LSM tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree) data. The data in [VDisks](#vdisk) and [local databases](#local-database) are organized in the form of an LSM tree. Therefore, there is a distinction between **VDisk compaction** and **Tablet compaction**. The compaction process is usually quite resource-intensive, so efforts are made to minimize the overhead associated with it, for example, by limiting the number of concurrent compactions.

#### gRPC proxy {#grpc-proxy}

A **gRPC Proxy** is the client proxy system for external user requests. Client requests enter the system via the [gRPC](https://grpc.io) protocol, then the proxy component translates them into internal calls for executing these requests, passed around via [Interconnect](#interconnect). This proxy provides an interface for both request-response and bidirectional streaming.

### Distributed storage implementation {#distributed-storage-implementation}

**Distributed storage** is a distributed fault-tolerant data storage layer that persists binary records called [LogoBlob](#logoblob), addressed by a particular type of identifier called [LogoBlobID](#logoblobid). Thus, distributed storage is a key-value store that maps LogoBlobID to a string up to 10MB in size. Distributed storage consists of many [storage groups](#storage-group), each being an independent data repository.

Distributed storage persists immutable data, with each immutable blob identified by a specific `LogoBlobID` key. The distributed storage API is very specific, designed only for use by [tablets](#tablet) to store their data and log changes, not for general-purpose data storage. Data in distributed storage is deleted using special barrier commands. Due to the lack of mutations in its interface, distributed storage can be implemented without implementing [distributed consensus](https://en.wikipedia.org/wiki/Consensus_(computer_science)). Moreover, distributed storage is just a building block tablets use to implement distributed consensus.

#### LogoBlob {#logoblob}

A **LogoBlob** is a piece of binary immutable data identified by [LogoBlobID](#logoblobid) and stored in [Distributed storage](#distributed-storage). The blob size is limited at the [VDisk](#vdisk) level and higher on the stack. Currently, the maximum blob size VDisks are ready to process is 10 MB.

#### LogoBlobID {#logoblobid}

A **LogoBlobID** is the [LogoBlob](#logoblob) identifier in the [Distributed storage](#distributed-storage). It has a structure of the form `[TabletID, Generation, Step, Channel, Cookie, BlobSize, PartID]`. The key elements of LogoBlobID are:
  
* `TabletID` is an [ID](#tabletid) of the tablet that the LogoBlob belongs to.
* `Generation` is the generation of the tablet in which the blob was recorded.
* `Channel` is the tablet [channel](#channel) where the LogoBlob is recorded.
* `Step` is an incremental counter, usually within the tablet generation.
* `Cookie` is a unique blob identifier within a single `Step`. A cookie is usually used when writing several blobs within a single `Step`.
* `BlobSize` is the LogoBlob size.
* `PartID` is the identifier of the blob part. It is crucial when the original LogoBlob is broken into parts using [erasure coding](#erasure-coding), and the parts are written to the corresponding [VDisks](#vdisk) and [storage groups](#storage-group).

#### Replication {#replication}

**Replication** is a process that ensures there are always enough copies (replicas) of data to maintain the desired availability characteristics of a {{ ydb-short-name }} cluster. Typically, it is used in geo-distributed {{ ydb-short-name }} clusters.

#### Erasure Coding {#erasure-coding}

[**Erasure coding**](https://en.wikipedia.org/wiki/Erasure_code) is a method of data encoding in which the original data is supplemented with redundancy and divided into several fragments, providing the ability to restore the original data if one or more fragments are lost. It is widely used in [single-AZ](#regions-az) {{ ydb-short-name }} clusters as opposed to [replication](#replication) with 3 replicas. For example, the most popular 4+2 scheme provides the same reliability as three replicas, with space redundancy of 1.5 versus 3.

#### PDisk {#pdisk}

**PDisk** or **Physical disk** is a component that controls a physical disk drive (block device). In other words, PDisk is a subsystem that implements an abstraction similar to a specialized file system on top of block devices (or files simulating a block device for testing purposes). PDisk provides data integrity controls (including [erasure encoding](#erasure-coding) of sector groups for data recovery on single bad sectors, integrity control with checksums), transparent data-at-rest encryption of all disk data, and transactional guarantees of disk operations (write confirmation strictly after `fsync`).

PDisk contains a scheduler that provides device bandwidth sharing between several clients ([VDisks](#vdisk)). PDisk divides a block device into chunks called [slots](#slot) (about 128 megabytes in size; smaller chunks are allowed). No more than 1 VDisk can own each slot at a time. PDisk also supports a recovery log shared between PDisk service records and all VDisks.

#### VDisk {#vdisk}

**VDisk** or **Virtual disk** is a component that implements the persistence of [distributed storage](#distributed-storage) [LogoBlobs](#logoblob) on [PDisks](#pdisk). VDisk stores all its data on PDisks. One VDisk corresponds to one PDisk, but usually, several VDisks are linked to one PDisk. Unlike PDisk, which hides chunks and logs behind it, VDisk provides an interface at the LogoBlob and [LogoBlobID](#logoblobid) level, like writing LogoBlob, reading LogoBlobID data, and deleting a set of LogoBlob using a special command. VDisk is a member of a [storage group](#storage-group). VDisk itself is local, but many VDisks in a given group provide reliable data storage. The VDisks in a group synchronize the data with each other and replicate the data in case of loss. A set of VDisks in a storage group forms a distributed RAID.

#### Yard {#yard}

**Yard** is the name of the [PDisk](#pdisk) API. It allows [VDisk](#vdisk) to read and write data to chunks and logs, reserve chunks, delete chunks, and transactionally receive and return ownership of chunks. In some contexts, Yard can be considered to be a synonym for PDisk.

#### Skeleton {#skeleton}

A **Skeleton** is an actor that provides an interface to a [VDisk](#vdisk).

#### SkeletonFront {#skeletonfront}

**SkeletonFront** is a proxy actor for Skeleton that controls the flow of messages coming to Skeleton.

#### Distributed storage controller {#ds-controller}

The **distributed storage controller** or **DS controller** manages the dynamic configuration of distributed storage, including information about [PDisks](#pdisk), [VDisks](#vdisk), and [storage groups](#storage-group). It interacts with [node wardens](#node-warden) to launch various distributed storage components. It interacts with [Hive](#hive) to allocate [channels](#channel) to [tablets](#tablet).

#### Proxy {#ds-proxy}

The **distributed storage proxy**, **DS proxy**, or **BS proxy** plays the role of a client library for performing operations with [Distributed storage](#distributed-storage). DS Proxy users are [tablets](#tablets) that write to and read from Distributed storage. DS Proxy hides the distributed nature of Distributed storage from the user. The task of DS Proxy is to write to the quorum of the [VDisks](#vdisk), make retries if necessary, and control the write/read flow to avoid overloading VDisks.

Technically, DS Proxy is implemented as an [actor service](#actor-service) launched by the [node warden](#node-warden) on each node for each storage group, processing all requests to the group (writing, reading, and deleting [LogoBlobs](#logoblob), blocking the group). When writing data, DS proxy performs [erasure encoding](#erasure-coding) of data by dividing LogoBlobs into parts, which are then sent to the corresponding VDisks. DS Proxy performs the reverse process when reading, receiving parts from VDisks, and restoring LogoBlobs from them.

#### Node warden {#node-warden}

**Node warden** or `BS_NODE` is an [actor service](#actor-service) on each node of the cluster, launching [PDisks](#pdisk), [VDisks](#vdisk), and [DS proxies](#proxy) of [static storage groups](#static-group) at the node start. Also, it interacts with the [DS controller](#ds-controller) to launch PDisk, VDisk, and DS proxies of [dynamic groups](#dynamic-group). The DS proxy of dynamic groups is launched on request: node warden processes "undelivered" messages to the DS proxy, launching the corresponding DS proxies and receiving the group configuration from the DS controller.

#### Fail realm {#fail-realm}

A **fail realm** is a set of [fail domains](#fail-domain) that are likely to fail simultaneously. The correlated failure of two [VDisks](#vdisk) within the same fail realm is more probable than that of two VDisks from different fail realms.

An example of a fail realm is a set of hardware located in the same [data center or availability zone](#regions-az) that can all fail together due to a natural disaster, major power outage, or similar event.

#### Fail domain {#fail-domain}

A **fail domain** is a set of hardware that may fail simultaneously. The correlated failure of two [VDisks](#vdisk) within the same fail domain is more probable than the failure of two VDisks from different fail domains. In the case of different fail domains, this probability is also affected by whether these domains belong to the same [fail realm](#fail-realm) or not.

For example, a fail domain includes disks on the same server, as all server disks may become unavailable if the server's PSU or network controller fails. A fail domain also typically includes servers located in the same server rack, as all the hardware in the rack may become unavailable if there is a power outage or an issue with the network hardware in the same rack. Thus, the typical fail domain corresponds to a server rack if the [cluster](#cluster) is configured to be rack-aware, or to a server otherwise.

Domain failures are handled automatically by {{ ydb-short-name }} without shutting down the cluster.

#### Distributed storage channel {#channel}

A **channel** is a logical connection between a [tablet](#tablet) and [Distributed storage](#distributed-storage) group. The tablet can write data to different channels, and each channel is mapped to a specific [storage group](#storage-group). Having multiple channels allows the tablet to:

* Record more data than one storage group can contain.
* Store different [LogoBlobs](#logoblobs) on different storage groups, with different properties like erasure encoding or on different storage media (HDD, SSD, NVMe).

### Distributed transactions implementation {#distributed-transaction-implementation}

Terms related to the implementation of [distributed transactions](#distributed-transactions) are explained below. The implementation itself is described in a separate article [{#T}](../contributor/datashard-distributed-txs.md).

#### Deterministic transactions {#deterministic-transactions}

{{ ydb-short-name }} distributed transactions are inspired by the research paper [Building Deterministic Transaction Processing Systems without Deterministic Thread Scheduling](http://cs-www.cs.yale.edu/homes/dna/papers/transactions-wodet11.pdf) by Alexander Thomson and Daniel J. Abadi from Yale University. The paper introduced the concept of **deterministic transaction** processing, which allows for highly efficient distributed processing of transactions. The original paper imposed limitations on what kinds of operations can be executed in this manner. As these limitations interfered with real-world user scenarios, {{ ydb-short-name }} evolved its algorithms to overcome them by using deterministic transactions as stages of executing user transactions with additional orchestration and locking.

#### Optimistic locking {#optimistic-locking}

As in many other database management systems, {{ ydb-short-name }} queries can put locks on certain pieces of data, like table rows, to ensure that concurrent access does not modify them into an inconsistent state. However, {{ ydb-short-name }} checks these locks not at the beginning of transactions but during commit attempts. The former is called **pessimistic locking** (used in PostgreSQL, for example), while the latter is called **optimistic locking** (used in {{ ydb-short-name }}).

#### Prepare stage {#prepare-stage}

The **prepare stage** is a phase of distributed transaction execution, during which the transaction body is registered on all participating shards.

#### Execute stage {#execute-stage}

The **execute stage** is a phase of distributed query execution in which the scheduled transaction is executed and the response is generated.

In some cases, instead of [prepare](#prepare-stage) and execute, the transaction is immediately executed, and a response is generated.  For example, this happens for transactions involving only one shard or consistent reads from a snapshot.

#### Dirty operations {#dirty-operations}

In the case of read-only transactions, similar to "read uncommitted" in other database management systems, it might be necessary to read data that has not yet been committed to disk. This is called **dirty operations**.

#### Read-write set {#rw-set}

The **read-write set** or **RW set** is a set of data that will participate in executing a [distributed transaction](#distributed-transactions). It combines the read set, the data that will be read, and the write set, the data modifications to be carried out.

#### Read set {#read-set}

The **read set** or **ReadSet data** is what participating shards forward during the transaction execution. In the case of data transactions, it may contain information about the state of [optimistic locks](#optimistic-locking), the readiness of the shard for commit, or the decision to cancel the transaction.

#### Transaction proxy {#transaction-proxy}

The **transaction proxy** or `TX_PROXY` is a service that orchestrates the execution of many [distributed transactions](#distributed-transactions): sequential phases, phase execution, planning, and aggregation of results. In the case of direct orchestration by other actors (for example, KQP data transactions), it is used for caching and allocation of unique [TxIDs](#txid).

#### Transaction flags {#txflags}

**Transaction flags** or **TxFlags** is a bitmask of flags that modify the execution of a transaction in some way.

#### Transaction ID {#txid}

**Transaction ID** or **TxID** is a unique identifier assigned to each transaction when it is accepted by {{ ydb-short-name }}.

#### Transaction order ID {#transaction-order-id}

A **transaction order ID** is a unique identifier assigned to each transaction during planning. It consists of [PlanStep](#planstep) and [Transaction ID](#txid).

#### PlanStep {#planstep}

**PlanStep** or **step** is the logical time for which a set of transactions is planned to be executed.

#### Mediator time {#mediator-time}

During the distributed query execution, **mediator time** is the logical time before which (inclusive) the shard participant must know the entire execution plan. It is used to advance the time in the absence of transactions on a particular shard, to determine whether it can read from a snapshot.

#### MiniKQL {#minikql}

**MiniKQL** is a language that allows the expression of a single [deterministic transaction](#deterministic-transactions) in the system. It is a functional, strongly typed language. Conceptually, the language describes a graph of reading from the database, performing calculations on the read data, and writing the results to the database and/or to a special document representing the query result (shown to the user). The MiniKQL transaction must explicitly set its read set (readable data) and assume a deterministic selection of execution branches (for example, there is no random).
  
MiniKQL is a low-level language. The system's end users only see queries in the [YQL](#yql) language, which relies on MiniKQL in its implementation.

#### KQP {#kqp}

**KQP** is a {{ ydb-short-name }} component responsible for the orchestration of user query execution and generating the final response.

### Global schema {#global-schema}

**Global Schema**, **Global Scheme**, or **Database Schema** is a schema of all the data stored in a [database](#database). It consists of [tables](#table) and other entities, such as [topics](#topic). The metadata about these entities is called a global schema. The term is used in contrast to **Local Schema**, which refers to the data schema inside a [tablet](#tablet). {{ ydb-short-name }} users never see the local schema and only work with the global schema.

### KiKiMR {#kikimr}

**KiKiMR** is the legacy name of {{ ydb-short-name }} that was used long before it became an [open-source product](https://github.com/ydb-platform/ydb). It can still be occasionally found in the source code, old articles and videos, etc.