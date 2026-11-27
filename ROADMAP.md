# YDB Roadmap
## Intro
The document contains high-level roadmap for YDB. Take a look at [👑 Epics Project](https://github.com/orgs/ydb-platform/projects/46/) also.

## Legend
We use the following symbols as abbreviations:

1. ㉓ - the feature appeared in the Roadmap for 2023;
1. ㉔ - the feature appeared in the Roadmap for 2024;
1. ㉕ - the feature appeared in the Roadmap for 2025;
1. ㉖ - the feature appeared in the Roadmap for 2026;
1. ㉗ - the feature appeared in the Roadmap for 2027;
1. ✅ - the feature has been released;
1. 🚧 - the feature is partially available and is under development;
1. ❌ - the feature has been refused;
1. 🐌 - the feature is taking longer than expected.

## Query Processor

1. ✅ ㉔ **Unique secondary indexes**
    * ✅ basic unique indexes released
    * 🚧 online index building (by default starting from 26.4)
1. ✅ ㉔ Apply **indexes automatically** to optimize data fetching (enabled by default in 24.4)
1. ✅ ㉔ **Default values** for table columns (constant defaults released in 24.3; arbitrary default expressions remain a separate task)
1. ✅ ㉔ **Asynchronous LLVM JIT** query compilation
1. ✅ ㉔ **Parameters in DECLARE clause are becoming optional**, better SQL compatibility (25.1)
1. 🚧 ㉔ **``INSERT INTO table FROM SELECT``** for large datasets (supported operations exist; removing large-write limits remains in progress)
1. 🚧 ㉔ Support for **transactional writes into both row and column tables** (mixed reads are supported; mixed row/column writes remain under development)
1. 🚧 ㉔ Support for **computed columns in a table** (STORED and VIRTUAL implementations exist behind feature flags; rollout is in progress)
1. 🚧 ㉔ Support for **temporary tables** (general YQL availability not established ❓)
1. ✅ ㉔ Support for **VIEW** SQL clause (enabled by default in 24.4)
1. ✅ ㉔ **Data Spilling** in case there is issufient amount of RAM (disk spilling released in 25.2; operator coverage and resource management continue to evolve)
1. 🚧 ㉔ **TPC-H, TPC-DS for 100TB** dataset
1. ✅ ㉓ Support for **Snapshot Readonly** transactions mode
1. 🚧 ㉓ **Better resource management** for KQP Resource Manager (share information about nodes resources, avoid OOMs)
    * ㉖ Reject YQL execution when there is a risk of an OOM failure
1. ✅ ㉓ Switch to **New Engine** for OLTP queries (QueryService is enabled by default)
1. ✅ ㉓ Support **`not null` for PK (primary key) table columns**
1. ✅ ㉓ **Aggregates and predicates push down to column-oriented tables**
1. ✅ ㉓ **Optimize data formats** for data transition between query phases
1. ✅ ㉓ **Index Rename/Rebuild**
1. ✅ ㉓ **KQP Session Actor** as a replacement for KQP Worker Actor (optimize to reduce CPU usage)
1. ㉓ Support a single **Database connection string** instead of multiple parameters ❓
1. ✅ ㉓ **Query Processor 3.0** (a set of tasks to be more like traditional database in case of query execution functionality)
    * ✅ ㉓ Support for **Streaming Lookup Join** via MVCC snapshots (avoid distributed transactions, scalability is better)
    * ✅ ㉓ **Universal API call for DML, DDL with unlimited results size for OLTP/OLAP workload** (aka ExecuteQuery) (QueryService released in 24.1; mixed DDL+DML statements remain a separate task)
    * ✅ ㉓ Support for **secondary indexes in ScanQuery** (ScanQuery is deprecated)
    * ✅ ㉓ **Transaction can see its own updates** (updates made during transaction execution are not buffered in RAM anymore, but rather are written to disk and available to read by this transaction)
1. ✅ ㉓ **Computation graphs caching (compute/datashard programs)** (optimize CPU usage)
1. 🚧 ㉓ **RPC Deadline & Cancellation propagation** (smooth timeout management) ❓
1. ✅ ㉓ **DDL for column-oriented tables**
1. 🚧 ㉔ **Query Optimizer v2**
    * ㉔ ✅ **Join order and join algorithm selection** for column tables
    * ㉖ 🚧 Universal **Rule based optimizer v2** for row and column tables
    * ㉖ 🚧 Universal **Cost based optimizer v2** for row and column tables
1. 🚧 ㉖ **Read-committed transaction isolation level**
1. 🚧 ㉕ **Workload Manager**
1. ✅ **Allow administrators to stop another user’s query**.
1. ✅ ㉖ **TRUNCATE TABLE**
1. ✅ ㉖ **`SET NOT NULL` for existing table columns**
1. ㉗ **SnapshotRW isolation level** (stronger than read-committed; snapshot reads and writes use a consistent snapshot)
1. ㉗ **CHECKSUM TABLE** (verify data integrity)
1. ㉗ **Functional (expression-based) secondary indexes** for row tables

## Database Core (Tablets, etc)
1. ✅ ㉔ **Exact Nearest Neighbor Vector Search**
1. ✅ ㉔ **Approximate Nearest Neighbor Vector Search**. [Global vector index](https://github.com/ydb-platform/ydb/issues/8967) (enabled by default in 25.2)
1. ✅ ㉔ **Volatile transactions**. YDB Distributed transactions v2, minimize network round trips in happy path (enabled by default in 24.4)
1. 🚧 ㉔ **Table statistics** for cost-based optimizer
    * ✅ ㉔ **Basic statistics** as table size and number of rows;
    * ㉕ **Column statistics** collected via **ANALYZE** (basic sampling, background auto-collection)
1. ✅ ㉔ **Evictable B-tree indexes for Row Tables** (enabled in 25.1)
1. ✅ ㉔ Reduce minimum requirements for **the number of cores to 2** for YDB dynamic node. Two-core testing support is recorded, a general production sizing requires 4 cores.
1. 🐌 ㉔ **Incremental backup** (incremental backup is disabled by default and is evolving)
1. 🐌 ㉔ **Point-in-time recovery** (on pause, return after incremental backup)
1. ✅ ㉔ **``ALTER TABLE ... ADD|DROP CHANGEFEED``**
1. 🚧 ㉔ **Async Replication** between YDB databases
    * 🚧 **Row Tables Async Replication**
        * ✅ **Row consistency**
        * 🚧 **Global consistency**
     * **Topics Async Replication**
     * **Column Tables Async Replication**
     * **Schema Changes Async Replication**
1. ✅ ㉔ Support for **Debezium** format (basic CDC format is implemented and documented)
1. 🚧 ㉔ **Topics autoscaling** (automatic partition increase is released; merge/scale-down remains a roadmap candidate)
1. ㉔ **Schema for YDB Topics**
1. ✅ ㉔ **Message-level parallelism** in YDB Topics
1. ✅ ㉓ Get **YDB topics** (aka pers queue, streams) ready for production
1. ✅ ㉓ Turn on **MVCC support** by default for row tables
1. ✅ ㉓ Enable **Snapshot read mode** by default (take and use MVCC snapshot for reads instead of running distributed transaction for reads)
1. ✅ ㉓ **Change Data Capture** (be able to get change feed of table updates)
1. ✅ ㉓ **Background compaction for DataShards**
1. ✅ ㉓ **Compressed Backups**. Add functionality to compress backup data
1. 🚧 ㉓ Process of **Extending State Storage** without cluster downtime. If a cluster grows from, say, 9 nodes to 900 State Storage configuration stays the same (9 nodes), it leads to a performance bottleneck.
1. 🐌 **Split/Merge DataShards *BY LOAD* by default**. Most users require this feature turned on by default
1. ✅ ㉓ **Transaction can see its own updates** (updates made during transaction execution are available for reading by this transaction)
1. ✅ ㉓ **Data Ingestion from topic to table** (transfers released in 25.2)
1. ㉓ Support **snapshot read over read replicas** (consistent reads against read replicas)
1. ✅ ㉓ **Transactions between topics and tables** (enabled by default for topics and row tables in 24.4)
1. ✅ ㉓ Support for **Kafka API compatible protocol** to YDB Topics
1. ✅ ㉕ **In-memory tables**
1. ✅ ㉖ **Transactional Locks Invalidated (TLI) resolution project** (reduce TLI errors for users, extended diagnostics)
1. ✅ ㉖ **Full-text search** in row tables (segmented index with compaction, range/PREFIX filtering, stop-words & synonyms, improved tokenizers)
1. 🚧 ㉖ **JSON GIN index**
1. ㉖ **HNSW vector index**
1. ㉗ **AI functions in YDB**: `ONNX` UDF, built-in embeddings
1. ㉖ **SST-level backup restore** (faster restore)
1. ㉖ **Per-table metrics** in YDB
1. ㉖ **Savepoints** in DataShards
1. ㉖ **Automatic data tiering** (hot/cold) for row tables
1. ㉗ **Consistent snapshot of one or more tables** (user-facing mechanism to take and use a consistent snapshot)
1. ㉗ **Split/Merge DataShards latency improvements** (move split/merge locks before commit, split by load on timeout-heavy shards)
storage
1. ㉗ **Range-based Bloom filters** (read performance on row tables)
1. ㉗ **Local index as a scheme object** (index ownership and management via schema)
1. ㉗ **Zstd compression** for row tables
1. ㉗ **Lock-free shared cache for tablets**, cache warm-up on tablet restart
1. ㉗ **BigString support in DataShard** and large BLOB/CLOB/UTF8 (>8MB)

### Hardcore or system wide
1. ✅ ㉔ **Tracing** capabilities (basic request tracing released in 24.3; user-facing KQP spans are a separate code-ready increment)
1. ㉔ Automatically **balance tablet channels** via BlobStorage groups
1. ✅ ㉓ **Datashard iterator reads via MVCC**
1. 🚧 ㉓ **Avoid Node Broker as SPF** (72-hour Data Plane operation without NodeBroker is confirmed; the broader emergency-mode project and the NBS-specific guarantee are not established as complete)
1. ㉓ **Subscriptions in SchemeBoard** (optimize interaction with SchemeBoard via subscription to updates)

## Security
1. ✅ ㉓ Basic LDAP Support
1. 🚧 ㉔ Support for OpenID Connect (included in 26.3)
1. ㉔ Authentication via KeyCloack
1. ✅ ㉔ Support for SASL framework (SCRAM-SHA-256 is implemented for Kafka/PG APIs; broader framework coverage is not established)
1. ✅ ㉕ **Built-in secrets storage** inside YDB
1. ㉖ **SSO rollout**: OIDC/OAuth authentication in **CLI and SDKs**
1. ㉗ **Encrypted backups of secrets**
1. ㉗ **Static credentials: faster repeated logins**
1. ㉗ **Fine-grained access control**: IAM-based object permissions, SQL permission inheritance, schema viewer access with/without schema rights, IDM integration for granting permissions
1. ㉗ **Security options hot-reload** from config without restart
1. ㉗ **Force user logout after inactivity** and refresh token support (SSO hardening)

## BlobStorage
1. ㉔ BlobStorage **latency optimization** (p999), less CPU consumption
1. ㉔ **ActorSystem performance optimizations**
1. ㉔ Optimize **ActorSystem for ARM processors**
1. 🚧 ㉔ **Effortless initial cluster deployment or Configuration V2** (Configuration V2/distconf exists; deployment and fleet migration work continues)
1. 🚧 ㉔ **Reduce number of BlobStorage groups** for a database (virtual-group decommission is included in 26.3 RC; object coverage and product rollout remain in progress)
    * ✅ ㉓ **BlobDepot**
1. ㉓ **"One leg" storage migration without downtime** (migrate 1/3 of the cluster from one AZ to another for mirror3-dc erasure encoding)
1. ✅ ㉓ **ActorSystem 1.5** (dynamically reassign threads in different thread pools)
1. ✅ ㉓ **Publish an utility for BlobStorage management** (it's called ds_tool for now, improve it and open)
1. ㉓ **Self-heal for degrated BlobStorage groups** (automatic self-heal for groups with two broken disks, get VDisk Donors production ready)
1. ✅ ㉓ **Avoid BSC (BlobStorage Controller) as SPF** (peer-to-peer configuration and caches are implemented)
1. ㉓ **BSC manages static group** (reconfiguration of the static BlobStorage group must be done BlobStorage Controller as for any other group)
1. ㉓ **(Semi-)Hard disk space separation** (Better guarantees for disk space usage by VDisks on a single PDisk)
1. ㉓ **Reduce space amplification** (Optimize storage layer)
1. ✅ ㉓ **Storage nodes decommission** (add ability to remove storage nodes)
1. 🚧 ㉕ **Bridge Cluster Configuration** (available, rolling out)
1. ㉖ **Mirror-3-dc degraded-mode operation for -2DCs** (support operation with two datacenters unavailable, reconfiguration is required to restore quorum)


## Analytical Capabilities
1. ✅ ㉔ **Backup** for column tables (from 26.3)
1. ㉔ Column tables **autosharding**
1. ㉓ 🚧 **Log Store** (log friendly column-oriented storage which allows to create 1+ million tables for logs storing)
1. ✅ ㉓ **Column-oriented Tables** (basic analytical capabilities released in 25.2; further operational and scale improvements continue)
1. 🚧 ㉓ **Tiered Storage for Column-oriented Tables** in S3 (internal use exists; public API and broader product readiness remain in progress)

## Federated Query
1. ✅ ㉓ **Deliver the first version**

## Embedded UI
Detailed roadmap could be found at [YDB Embedded UI repo](https://github.com/ydb-platform/ydb-embedded-ui/blob/main/ROADMAP.md).

## Command Line Interface
1. ✅ ㉔ Use a **single `ydb sql` command** instead of `ydb yql`, `ydb table query` or `ydb scripting` to execute any query
1. ✅ ㉓ Interactive CLI

## Tests and Benchmarks
1. ㉓ **Built-in load test for DataShards** in YCSB manner
1. ✅ ㉓ **`ydb workload` for topics**
1. ✅ ㉔ **Jepsen tests support** [Blog post](https://blog.ydb.tech/hardening-ydb-with-jepsen-lessons-learned-e3238a7ef4f2)

## Refused Items
1. ❌ *(refused)* Try **RTMR-tablet** for key-value workload. This effort explored using RTMR-tablet to enable fast parallel reads. We have since designated RTMR-tablet as legacy and are focusing on DataShard.
1. ❌ *(refused)* Support **constraints in query optimizer**. This item will not be implemented as currently phrased, as its scope is too vague. It will be replaced with more specific, clearly defined tasks.
1. ❌ *(refused for now)* **PostgreSQL compatibility**. This effort aimed to provide PostgreSQL compatibility by embedding PostgreSQL source code into YDB and virtualizing data types. We discontinued this work in favor of a PostgreSQL FDW-based approach.
1. ❌ *(refused)* ㉓ **Switch to TRope** (or don't use TString/std::string directly, provide zero-copy data passing between components)
