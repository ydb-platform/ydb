# YDB Roadmap

## Query Processor
1. Support for **Snapshot Readonly** transactions mode
1. **Better resource management** for KQP Resource Manager (share information about nodes resources, avoid OOMs)
1. Switch to **New Engine** for OLTP queries
1. ✅ Support **`not null` for PK (private key) table columns**
1. **Aggregates and predicates push down to column-oriented tables**
1. **Optimize data formats** for data transition between query phases
1. **Index Rename/Rebuild**
1. **KQP Session Actor** as a replacement for KQP Worker Actor (optimize to reduce CPU usage)
1. **PostgreSQL compatibility**
    * Support PostgreSQL datatypes **serialization/deserialization** in YDB Public API
    * PostgreSQL compatible **query execution** (TPC-C, TPC-H queries should work)
    * Support for PostgreSQL **wire protocol**
1. Support a single **Database connection string** instead of multiple parameters
1. Support **constraints in query optimizer**
1. **Query Processor 3.0** (a set of tasks to be more like traditional database in case of query execution functionality)
    * Support for **Streaming Lookup Join** via MVCC snapshots (avoid distributed transactions, scalability is better)
    * **Universal API call for DML, DDL with unlimited results size** (aka StreamExecuteQuery, which allows to execute each query)
    * Support for **secondary indexes in ScanQuery**
    * **Transaction can see its own updates** (updates made during transaction execution are not buffered in RAM anymore, but rather are written to disk and available to read by this transaction)
1. **Computation graphs caching (compute/datashard programs)** (optimize CPU usage)
1. **RPC Deadline & Cancellation propagation** (smooth timeout management)
1. **DDL for column-oriented tables**

## Database Core (Tablets, etc)
1. Get **YDB topics** (aka pers queue, streams) ready for production
1. ✅ Turn on **MVCC support** by default
1. Enable **Snapshot read mode** by default (take and use MVCC snapshot for reads instead of running distributed transaction for reads)
1. **Change Data Capture** (be able to get change feed of table updates)
1. **Async Replication** between YDB databases
1. ✅ **Background compaction for DataShards**
1. **Compressed Backups**. Add functionality to compress backup data
1. Process of **Extending State Storage** without cluster downtime. If a cluster grows from, say, 9 nodes to 900 State Storage configuration stays the same (9 nodes), it leads to a performance bottleneck.
1. **Splite/Merge DataShards *BY LOAD* by default**. Most users require this feature turned on by default
1. Support **PostgreSQL datatypes** in tablet local database
1. **Basic histogram for DataShards** (first step towards cost based optimizations)
1. **Transaction can see its own updates** (updates made during transaction execution are not buffered in RAM anymore, but rather are written to disk and available to read by this transaction)
1. **Data Ingestion from topic to table** (implement built-in compatibility to ingest data to YDB tables from topics)
1. Support **snapshot read over read replicas** (consistent reads against read replicas)

### Hardcore
1. **Datashard iterator reads via MVCC**
1. **Switch to TRope** (or don't use TString/std::string directly, provide zero-copy data passing between components)
1. **Avoid Node Broker as SPF** (NBS must work without Node Broker under emergency conditions)
1. **Subscriptions in SchemeBoard** (optimize interaction with SchemeBoard via subsription to updates)

## BlobStorage
1. **"One leg" storage migration without downtime** (migrate 1/3 of the cluster from one AZ to another for mirror3-dc erasure encoding)
1. **ActorSystem 1.5** (dynamically reassign threads in different thread pools)
1. **Publish an utility for BlobStorage management** (it's called ds_tool for now, improve it and open)
1. **Self-heal for degrated BlobStorage groups** (automatic self-heal for groups with two broken disks, get VDisk Donors production ready)
1. **BlobDepot** (a component for smooth blobs management between groups)
1. **Avoid BSC (BlobStorage Controller) as SPF** (be able to run the cluster without BSC in emergency cases)
1. **BSC manages static group** (reconfiguration of the static BlobStorage group must be done BlobStorage Controller as for any other group)
1. **(Semi-)Hard disk space separation** (Better guarantees for disk space usage by VDisks on a single PDisk)
1. **Reduce space amplification** (Optimize storage layer)
1. **Storage nodes decommission** (Add ability to remove storage nodes)

## Analytical Capabilities
1. **Log Store** (log friendly column-oriented storage which allows to create 1+ million tables for logs storing)
1. **Column-oriented Tables** (introduce a Column-oriented tables in additon to Row-orinted tables)
1. **Tiered Storage for Column-oriented Tables** (with the ability to store the data in S3)

## Federated Query
1. **Run the first version**

## Embedded UI
1.  **Support for all schema entities**
    * **YDB Topics Support** (add support for viewing metadata of YDB topics, its data, lag, etc)
    * **CDC Streams**
    * **Secondary Indexes**
    * **Read Replicas**
    * **Column-oriented Tables**

## Command Line Utility
1. Use a **single `ydb yql`** instead of `ydb table query` or `ydb scripting`
1. Interactive CLI

## Tests and Benchmarks
1. **Built-in load test for DataShards** in YCSB manner
1. **`ydb workload` for topics**
1. **Jepsen tests support**

## Experiments
1. Try **RTMR-tablet** for key-value workload
