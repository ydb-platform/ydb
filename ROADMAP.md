# YDB Roadmap
## Intro
The document contains high-level roadmap for YDB. Take a look at [👑 Epics Project](https://github.com/orgs/ydb-platform/projects/46/) also.

Status review: **2026-11-15**. See [historical and current roadmap with sources](ROADMAP-mars.md). Release versions are not calendar-year labels; RC/feature-gated implementations remain marked as partial.
## Legend
We use the following symbols as abbreviations:

1. ㉓ - feature appeared in the Roadmap for 2023;
1. ㉔ - feature appeared in the Roadmap for 2024;
1. ✅ - feature has been released;
1. 🚧 - feature is partially available and is under development;
1. ❌ - feature has been refused;
1. 🔥 - not yet released, but we are in rush.

## Query Processor

1. ✅ ㉔ **Unique secondary indexes** (basic unique indexes released; online index building is a separate, feature-gated increment)
1. ✅ ㉔ Apply **indexes automatically** to optimize data fetching (enabled by default in 24.4.4.2)
1. ✅ ㉔ **Default values** for table columns (constant defaults released in 24.3; arbitrary default expressions remain a separate task)
1. ㉔ **Asynchronous LLVM JIT** query compilation
1. ✅ ㉔ **Parameters in DECLARE clause are becoming optional**, better SQL compatibility (25.1)
1. ✅ ㉔ **Cost-based optimizer** for join order and join algorithm selection (enabled by default for queries using column tables in 25.2; further OLTP/new optimizer work continues)
1. 🚧 ㉔ **``INSERT INTO table FROM SELECT``** for large datasets (supported operations exist; removing large-write limits remains in progress)
1. 🚧 ㉔ Support for **transactional writes into both row and column tables** (mixed reads are supported; mixed row/column writes remain under development)
1. 🚧 ㉔ Support for **computed columns in a table** (STORED and VIRTUAL implementations exist behind feature flags; rollout is in progress)
1. 🚧 ㉔ Support for **temporary tables** (PG CREATE TEMP TABLE implementation confirmed; general YQL availability not established)
1. ✅ ㉔ Support for **VIEW** SQL clause (enabled by default in 24.4.4.2)
1. ✅ ㉔ **Data Spilling** in case there is issufient amount of RAM (disk spilling released in 25.2; operator coverage and resource management continue to evolve)
1. 🚧 ㉔ **TPC-H, TPC-DS for 100TB** dataset (TPC-H 100 TB milestone is recorded; the combined TPC-H/TPC-DS 100 TB goal is not confirmed complete)
1. ✅ ㉓ Support for **Snapshot Readonly** transactions mode
1. 🚧 ㉓ **Better resource management** for KQP Resource Manager (share information about nodes resources, avoid OOMs)
1. ✅ ㉓ Switch to **New Engine** for OLTP queries
1. ✅ ㉓ Support **`not null` for PK (primary key) table columns**
1. ✅ ㉓ **Aggregates and predicates push down to column-oriented tables**
1. ✅ ㉓ **Optimize data formats** for data transition between query phases
1. ✅ ㉓ **Index Rename/Rebuild**
1. ✅ ㉓ **KQP Session Actor** as a replacement for KQP Worker Actor (optimize to reduce CPU usage)
1. **PostgreSQL compatibility**
    * ✅ ㉓ Support PostgreSQL datatypes **serialization/deserialization** in YDB Public API
    * 🚧 ㉓ PostgreSQL compatible **query execution** (TPC-C, TPC-H queries should work)
    * ✅ ㉓ Support for PostgreSQL **wire protocol**
1. ㉓ Support a single **Database connection string** instead of multiple parameters
1. ㉓ Support **constraints in query optimizer**
1. **Query Processor 3.0** (a set of tasks to be more like traditional database in case of query execution functionality)
    * ㉓ Support for **Streaming Lookup Join** via MVCC snapshots (avoid distributed transactions, scalability is better)
    * ✅ ㉓ **Universal API call for DML, DDL with unlimited results size for OLTP/OLAP workload** (aka ExecuteQuery) (QueryService released in 24.1; mixed DDL+DML statements remain a separate task)
    * ✅ ㉓ Support for **secondary indexes in ScanQuery**
    * ✅ ㉓ **Transaction can see its own updates** (updates made during transaction execution are not buffered in RAM anymore, but rather are written to disk and available to read by this transaction)
1. ✅ ㉓ **Computation graphs caching (compute/datashard programs)** (optimize CPU usage)
1. 🚧 ㉓ **RPC Deadline & Cancellation propagation** (smooth timeout management)
1. ✅ ㉓ **DDL for column-oriented tables**

## Database Core (Tablets, etc)
1. ✅ ㉔ **Exact Nearest Neighbor Vector Search**
1. ✅ ㉔ **Approximate Nearest Neighbor Vector Search**. [Global vector index](https://github.com/ydb-platform/ydb/issues/8967) (25.1; enabled by default in 25.2)
1. ✅ ㉔ **Volatile transactions**. YDB Distributed transactions 2.0, minimize network round trips in happy path (enabled by default in 24.4.4.2)
1. ✅ ㉔ **Table statistics** for cost-based optimizer (basic statistics implemented; collection and optimizer integration continue to evolve)
1. 🚧 ㉔ **Memory optimization for row tables** (evictable B-tree indexes replacing full [SST index loading](https://github.com/ydb-platform/ydb/issues/1483) are enabled in 25.1; broader memory optimization continues)
1. 🚧 ㉔ Reduce minimum requirements for **the number of cores to 2** for YDB node (two-core testing support is recorded; a general production sizing guarantee is not established)
1. 🚧 ㉔ **Incremental backup** and **Point-in-time recovery** (incremental backup exists but is disabled by default in 26.2; PITR is a separate open task and is excluded from incremental backup v0)
1. 🚧 ㉔ **``ALTER CHANGEFEED``** (changing CDC retention is released; the broader alteration task remains open)
1. ㉔ **Async Replication** between YDB databases (column tables, topics)
1. 🚧 ㉔ **Async Replication** between YDB databases (schema changes) (implementation and object coverage remain in progress)
1. 🚧 ㉔ Support for **Debezium** format (basic CDC format is implemented and documented; compatibility improvements remain open)
1. 🚧 ㉔ **Topics autoscaling** (automatic partition increase is released; merge/scale-down remains a roadmap candidate)
1. 🚧 ㉔ **Extended Kafka API** protocol to YDB Topics support (native reader balancing and other extensions are released; the complete original v19 criterion is not independently established)
1. ㉔ **Schema for YDB Topics**
1. ㉔ **Message-level parallelism** in YDB Topics
1. ✅ ㉓ Get **YDB topics** (aka pers queue, streams) ready for production
1. ✅ ㉓ Turn on **MVCC support** by default
1. ✅ ㉓ Enable **Snapshot read mode** by default (take and use MVCC snapshot for reads instead of running distributed transaction for reads)
1. ✅ ㉓ **Change Data Capture** (be able to get change feed of table updates)
1. ✅ ㉓ **Async Replication** between YDB databases  (first version, row tables, w/o schema changes) (released in 24.3; later coverage and reliability work continues)
1. ✅ ㉓ **Background compaction for DataShards**
1. ✅ ㉓ **Compressed Backups**. Add functionality to compress backup data
1. 🚧 ㉓ Process of **Extending State Storage** without cluster downtime. If a cluster grows from, say, 9 nodes to 900 State Storage configuration stays the same (9 nodes), it leads to a performance bottleneck. (Automatic reconfiguration/scaling is recorded in 25-3 and testing; full production rollout is not established.)
1. **Split/Merge DataShards *BY LOAD* by default**. Most users require this feature turned on by default
1. ✅ ㉓ Support **PostgreSQL datatypes** in tablet local database
1. **Basic histogram for DataShards** (first step towards cost based optimizations)
1. ✅ ㉓ **Transaction can see its own updates** (updates made during transaction execution are not buffered in RAM anymore, but rather are written to disk and available to read by this transaction)
1. ✅ ㉓ **Data Ingestion from topic to table** (transfers released in 25.2)
1. ㉓ Support **snapshot read over read replicas** (consistent reads against read replicas)
1. ✅ ㉓ **Transactions between topics and tables** (enabled by default for topics and row tables in 24.4.4.2)
1. ✅ ㉓ Support for **Kafka API compatible protocol** to YDB Topics

### Hardcore or system wide
1. ✅ ㉔ **Tracing** capabilities (basic request tracing released in 24.3; user-facing KQP spans are a separate code-ready increment)
1. ㉔ Automatically **balance tablet channels** via BlobStorage groups
1. ✅ ㉓ **Datashard iterator reads via MVCC**
1. ❌ *(refused)* ㉓ **Switch to TRope** (or don't use TString/std::string directly, provide zero-copy data passing between components)
1. 🚧 ㉓ **Avoid Node Broker as SPF** (72-hour Data Plane operation without NodeBroker is confirmed; the broader emergency-mode project and the NBS-specific guarantee are not established as complete)
1. ㉓ **Subscriptions in SchemeBoard** (optimize interaction with SchemeBoard via subscription to updates)

## Security
1. ✅ ㉓ Basic LDAP Support
1. 🚧 ㉔ Support for OpenID Connect (included in 26.3 RC; general stable-release availability is not established in the checked changelog)
1. ㉔ Authentication via KeyCloack
1. 🚧 ㉔ Support for SASL framework (SCRAM-SHA-256 is implemented for Kafka/PG APIs; broader framework coverage is not established)

## BlobStorage
1. ㉔ BlobStorage **latency optimization** (p999), less CPU consumption
1. ㉔ **ActorSystem performance optimizations**
1. ㉔ Optimize **ActorSystem for ARM processors**
1. 🚧 ㉔ **Effortless initial cluster deployment** (Configuration V2/distconf exists; deployment and fleet migration work continues)
1. 🚧 ㉔ **Reduce number of BlobStorage groups** for a database (virtual-group decommission is included in 26.3 RC; object coverage and product rollout remain in progress)
1. ㉓ **"One leg" storage migration without downtime** (migrate 1/3 of the cluster from one AZ to another for mirror3-dc erasure encoding)
1. ✅ ㉓ **ActorSystem 1.5** (dynamically reassign threads in different thread pools)
1. ✅ ㉓ **Publish an utility for BlobStorage management** (it's called ds_tool for now, improve it and open)
1. ㉓ **Self-heal for degrated BlobStorage groups** (automatic self-heal for groups with two broken disks, get VDisk Donors production ready)
1. 🚧 ㉓ **BlobDepot** (MVP and specific deployments exist; integration and decommission coverage remain in progress)
1. 🚧 ㉓ **Avoid BSC (BlobStorage Controller) as SPF** (peer-to-peer configuration and caches are implemented; the full emergency/restart scenario is not established as complete)
1. ㉓ **BSC manages static group** (reconfiguration of the static BlobStorage group must be done BlobStorage Controller as for any other group)
1. ㉓ **(Semi-)Hard disk space separation** (Better guarantees for disk space usage by VDisks on a single PDisk)
1. ㉓ **Reduce space amplification** (Optimize storage layer)
1. ✅ ㉓ **Storage nodes decommission** (Add ability to remove storage nodes)

## Analytical Capabilities
1. 🚧 ㉔ **Backup** for column tables (disabled by default in 26.2; export/import included in 26.3 RC)
1. ㉔ Column tables **autosharding**
1. ㉓ 🚧 **Log Store** (log friendly column-oriented storage which allows to create 1+ million tables for logs storing)
1. ✅ ㉓ **Column-oriented Tables** (basic analytical capabilities released in 25.2; further operational and scale improvements continue)
1. 🚧 ㉓ **Tiered Storage for Column-oriented Tables** (internal use exists; public API and broader product readiness remain in progress)

## Federated Query
1. ✅ ㉓ **Run the first version**

## Embedded UI
Detailed roadmap could be found at [YDB Embedded UI repo](https://github.com/ydb-platform/ydb-embedded-ui/blob/main/ROADMAP.md).

## Command Line Interface
1. ✅ ㉔ Use a **single `ydb sql` command** instead of `ydb yql`, `ydb table query` or `ydb scripting` to execute any query
1. ✅ ㉓ Interactive CLI

## Tests and Benchmarks
1. ㉓ **Built-in load test for DataShards** in YCSB manner
1. ✅ ㉓ **`ydb workload` for topics**
1. ✅ ㉔ **Jepsen tests support** [Blog post](https://blog.ydb.tech/hardening-ydb-with-jepsen-lessons-learned-e3238a7ef4f2)

## Experiments
1. ❌ *(refused)* Try **RTMR-tablet** for key-value workload




updates:

- Conf V2
- Correlated Subqueries
