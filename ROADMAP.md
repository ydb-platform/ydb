# YDB Roadmap
## Intro
The document contains high-level roadmap for YDB. Take a look at [👑 Epics Project](https://github.com/orgs/ydb-platform/projects/46/) also.
## Legend
We use the following symbols as abbreviations:

1. ㉓ - feature appeared in the Roadmap for 2023;
1. ㉔ - feature appeared in the Roadmap for 2024;
1. ✅ - feature has been released;
1. 🚧 - feature is partially available and is under development;
1. ❌ - feature has been refused;
1. 🔥 - not yet released, but we are in rush.

## Query Processor

1. ㉔ **Unique secondary indexes**
1. ㉔ Apply **indexes automatically** to optimize data fetching
1. ㉔ **Default values** for table columns
1. ㉔ **Asynchronous LLVM JIT** query compilation
1. ㉔ **Parameters in DECLARE clause are becoming optional**, better SQL compatibility
1. 🚧㉔ **Cost-based optimizer** for join order and join algorithm selection
1. ㉔ **``INSERT INTO table FROM SELECT``** for large datasets
1. ㉔ Support for **transactional writes into both row and column tables**
1. ㉔ Support for **computed columns in a table**
1. ㉔ Support for **temporary tables**
1. ㉔ Support for **VIEW** SQL clause
1. ㉔ **Data Spilling** in case there is issufient amount of RAM
1. ㉔ **TPC-H, TPC-DS for 100TB** dataset
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
    * ㉓ **Universal API call for DML, DDL with unlimited results size for OLTP/OLAP workload** (aka ExecuteQuery)
    * ✅ ㉓ Support for **secondary indexes in ScanQuery**
    * ✅ ㉓ **Transaction can see its own updates** (updates made during transaction execution are not buffered in RAM anymore, but rather are written to disk and available to read by this transaction)
1. ✅ ㉓ **Computation graphs caching (compute/datashard programs)** (optimize CPU usage)
1. 🚧 ㉓ **RPC Deadline & Cancellation propagation** (smooth timeout management)
1. ✅ ㉓ **DDL for column-oriented tables**

## Database Core (Tablets, etc)
1. ✅ ㉔ **Exact Nearest Neighbor Vector Search**
1. ㉔ **Approximate Nearest Neighbor Vector Search**. [Global vector index](https://github.com/ydb-platform/ydb/issues/8967)
1. ㉔ **Volatile transactions**. YDB Distributed transactions 2.0, minimize network round trips in happy path
1. ㉔ **Table statistics** for cost-based optimizer
1. ㉔ **Memory optimization for row tables** (avoid full [SST index loading](https://github.com/ydb-platform/ydb/issues/1483), dynamic cache adjusting)
1. ㉔ Reduce minimum requirements for **the number of cores to 2** for YDB node
1. ㉔ **Incremental backup** and **Point-in-time recovery**
1. ㉔ **``ALTER CHANGEFEED``**
1. ㉔ **Async Replication** between YDB databases (column tables, topics)
1. ㉔ **Async Replication** between YDB databases (schema changes)
1. ㉔ Support for **Debezium** format
1. ㉔ **Topics autoscaling** (increase/decrease number of partitions in the topic automatically)
1. ㉔ **Extended Kafka API** protocol to YDB Topics support (balance reads, support for v19)
1. ㉔ **Schema for YDB Topics**
1. ㉔ **Message-level parallelism** in YDB Topics
1. ✅ ㉓ Get **YDB topics** (aka pers queue, streams) ready for production
1. ✅ ㉓ Turn on **MVCC support** by default
1. ✅ ㉓ Enable **Snapshot read mode** by default (take and use MVCC snapshot for reads instead of running distributed transaction for reads)
1. ✅ ㉓ **Change Data Capture** (be able to get change feed of table updates)
1. 🔥 ㉓ **Async Replication** between YDB databases  (first version, row tables, w/o schema changes)
1. ✅ ㉓ **Background compaction for DataShards**
1. ✅ ㉓ **Compressed Backups**. Add functionality to compress backup data
1. ㉓ Process of **Extending State Storage** without cluster downtime. If a cluster grows from, say, 9 nodes to 900 State Storage configuration stays the same (9 nodes), it leads to a performance bottleneck.
1. **Split/Merge DataShards *BY LOAD* by default**. Most users require this feature turned on by default
1. ✅ ㉓ Support **PostgreSQL datatypes** in tablet local database
1. **Basic histogram for DataShards** (first step towards cost based optimizations)
1. ✅ ㉓ **Transaction can see its own updates** (updates made during transaction execution are not buffered in RAM anymore, but rather are written to disk and available to read by this transaction)
1. ㉓ **Data Ingestion from topic to table** (implement built-in compatibility to ingest data to YDB tables from topics)
1. ㉓ Support **snapshot read over read replicas** (consistent reads against read replicas)
1. ㉓ 🚧 **Transactions between topics and tables**
1. ✅ ㉓ Support for **Kafka API compatible protocol** to YDB Topics

### Hardcore or system wide
1. ㉔ **Tracing** capabilities
1. ㉔ Automatically **balance tablet channels** via BlobStorage groups
1. ✅ ㉓ **Datashard iterator reads via MVCC**
1. ❌ *(refused)* ㉓ **Switch to TRope** (or don't use TString/std::string directly, provide zero-copy data passing between components)
1. ㉓ **Avoid Node Broker as SPF** (NBS must work without Node Broker under emergency conditions)
1. ㉓ **Subscriptions in SchemeBoard** (optimize interaction with SchemeBoard via subsription to updates)

## Security
1. ✅ ㉓ Basic LDAP Support
1. ㉔ Support for OpenID Connect
1. ㉔ Authentication via KeyCloack
1. ㉔ Support for SASL framework

## BlobStorage
1. ㉔ BlobStorage **latency optimization** (p999), less CPU consumption
1. ㉔ **ActorSystem performance optimizations**
1. ㉔ Optimize **ActorSystem for ARM processors**
1. ㉔ **Effortless initial cluster deployment** (provide only nodes and disks description)
1. ㉔ **Reduce number of BlobStorage groups** for a database (add ability to remove unneeded groups)
1. ㉓ **"One leg" storage migration without downtime** (migrate 1/3 of the cluster from one AZ to another for mirror3-dc erasure encoding)
1. ✅ ㉓ **ActorSystem 1.5** (dynamically reassign threads in different thread pools)
1. ✅ ㉓ **Publish an utility for BlobStorage management** (it's called ds_tool for now, improve it and open)
1. ㉓ **Self-heal for degrated BlobStorage groups** (automatic self-heal for groups with two broken disks, get VDisk Donors production ready)
1. ㉓ **BlobDepot** (a component for smooth blobs management between groups)
1. ㉓ **Avoid BSC (BlobStorage Controller) as SPF** (be able to run the cluster without BSC in emergency cases)
1. ㉓ **BSC manages static group** (reconfiguration of the static BlobStorage group must be done BlobStorage Controller as for any other group)
1. ㉓ **(Semi-)Hard disk space separation** (Better guarantees for disk space usage by VDisks on a single PDisk)
1. ㉓ **Reduce space amplification** (Optimize storage layer)
1. ✅ ㉓ **Storage nodes decommission** (Add ability to remove storage nodes)

## Analytical Capabilities
1. ㉔ **Backup** for column tables
1. ㉔ Column tables **autosharding**
1. ㉓ 🚧 **Log Store** (log friendly column-oriented storage which allows to create 1+ million tables for logs storing)
1. ㉓ 🚧 **Column-oriented Tables** (introduce a Column-oriented tables in additon to Row-orinted tables)
1. ㉓ **Tiered Storage for Column-oriented Tables** (with the ability to store the data in S3)

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
