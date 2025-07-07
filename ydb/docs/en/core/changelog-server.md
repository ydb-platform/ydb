# {{ ydb-short-name }} Server changelog

## Version 24.4 {#24-4}

### Version 24.4.4.12 {#24-4-4-12}

Release date: June 3, 2025.

## Performance

* [Limited](https://github.com/ydb-platform/ydb/pull/17755) the number of internal inflight configuration updates.
* [Optimized](https://github.com/ydb-platform/ydb/issues/18289) memory consumption by PQ tablets.
* [Optimized](https://github.com/ydb-platform/ydb/issues/18473) CPU consumption of Scheme shard and reduced query latencies by checking operation count limits before performing tablet split and merge operations.

## Bug Fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/17123) a rare issue of client applications hanging during transaction commit where deleting partition had been done before write quota update.
* [Fixed](https://github.com/ydb-platform/ydb/pull/17312) an error in copying tables with Decimal type, which caused failures when rolling back to a previous version.
* [Fixed](https://github.com/ydb-platform/ydb/pull/17519) an [issue](https://github.com/ydb-platform/ydb/issues/17499) where a commit without confirmation of writing to a topic led to the blocking of the current and subsequent transactions with topics.
* Fixed transaction hanging when working with topics during tablet [restart](https://github.com/ydb-platform/ydb/issues/17843) or [deletion](https://github.com/ydb-platform/ydb/issues/17915).
* [Fixed](https://github.com/ydb-platform/ydb/pull/18114) [issues](https://github.com/ydb-platform/ydb/issues/18071) with reading messages larger than 6Mb via [Kafka API](./reference/kafka-api).
* [Fixed](https://github.com/ydb-platform/ydb/pull/18319) memory leak during writing to the [topic](./concepts/glossary#topic).
* Fixed errors in processing [nullable columns](https://github.com/ydb-platform/ydb/issues/15701) and [columns with UUID type](https://github.com/ydb-platform/ydb/issues/15697) in row tables.

### Version 24.4.4.2 {#24-4-4-2}

Release date: April 15, 2025

#### Functionality

* Enabled by default:

  * support for [views](./concepts/datamodel/view.md)
  * [auto-partitioning mode](./concepts/topic.md#autopartitioning) for topics
  * [transactions involving topics and row-oriented tables simultaneously](./concepts/transactions.md#topic-table-transactions)
  * [volatile distributed transactions](./contributor/datashard-distributed-txs.md#volatile-transactions)


* Added the ability to [read and write to a topic](./reference/kafka-api/examples.md#kafka-api-usage-examples) using the Kafka API without authentication.

#### Performance

* Enabled by default automatic secondary index selection for queries.

#### Bug Fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/14811) an error that led to a significant decrease in reading speed from [tablet followers](./concepts/glossary.md#tablet-follower).
* [Fixed](https://github.com/ydb-platform/ydb/pull/14516) an error that caused volatile distributed transactions to sometimes wait for confirmations until the next reboot.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15077) a rare assertion failure (server process crash) when followers attached to leaders with an inconsistent snapshot.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15074) a rare datashard crash when a dropped table shard is restarted with uncommitted persistent changes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15194) an error that could disrupt the order of message processing in a topic.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15308) a rare error that could stop reading from a topic partition.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15160) an issue where a transaction could hang if a user performed a control plane operation on a topic (for example, adding partitions or a consumer) while the PQ tablet is moving to another node.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15233) a memory leak issue with the UserInfo counter value. Because of the memory leak, a reading session would eventually return a "too big in flight" error.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15467) a proxy crash due to duplicate topics in a request.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15933) a rare bug where a user could write to a topic without any account quota being applied or consumed.
* [Fixed](https://github.com/ydb-platform/ydb/pull/16288) an issue where topic deletion returned "OK" while the topic tablets persisted in a functional state. To remove such tablets, follow the instructions from the [pull request](https://github.com/ydb-platform/ydb/pull/16288).
* [Fixed](https://github.com/ydb-platform/ydb/pull/16418) a rare issue that prevented the restoration of a backup for a large secondary indexed table.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15862) an issue that caused errors when inserting data using `UPSERT` into row-oriented tables with default values.
* [Resolved](https://github.com/ydb-platform/ydb/pull/15334) a bug that caused failures when executing queries to tables with secondary indexes that returned result lists using the RETURNING * expression.

## Version 24.3 {#24-3}

### Version 24.3.15.5 {#24-3-15-5}

Release date: February 6, 2025

#### Functionality

* Added the ability to register a [database node](./concepts/glossary.md#database-node) using a certificate. In the [Node Broker](./concepts/glossary.md#node-broker) the flag `AuthorizeByCertificate` has been added to enable certificate-based registration.
* [Added](https://github.com/ydb-platform/ydb/pull/11775) priorities for authentication ticket through a [third-party IAM provider](./security/authentication.md#iam), with the highest priority given to requests from new users. Tickets in the cache update their information with a lower priority.

#### Performance

* [Improved](https://github.com/ydb-platform/ydb/pull/12747) tablet startup time on large clusters: 210 ms → 125 ms (SSD), 260 ms → 165 ms (HDD).

#### Bug Fixes

* [Removed](https://github.com/ydb-platform/ydb/pull/11901) the restriction on writing values greater than 127 to the Uint8 type.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12221) an issue where reading small messages from a topic in small chunks significantly increased CPU load, which could lead to delays in reading and writing to the topic.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12915) an issue with restoring from a backup stored in S3 with path-style addressing.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13222) an issue with restoring from a backup that was created during an automatic table split.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12601) an issue with Uuid serialization for [CDC](./concepts/cdc.md).
* [Fixed](https://github.com/ydb-platform/ydb/pull/12018) an issue with ["frozen" locks](./contributor/datashard-locks-and-change-visibility.md#interaction-with-distributed-transactions), which could be caused by bulk operations (e.g., TTL-based deletions).
* [Fixed](https://github.com/ydb-platform/ydb/pull/12804) an issue where reading from a follower of tablets sometimes caused crashes during automatic table splits.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12807) an issue where the [coordination node](./concepts/datamodel/coordination-node.md) successfully registered proxy servers despite a connection loss.
* [Fixed](https://github.com/ydb-platform/ydb/pull/11593) an issue that occurred when opening the Embedded UI tab with information about [distributed storage groups](./concepts/glossary.md#storage-group).
* [Fixed](https://github.com/ydb-platform/ydb/pull/12448) an issue where the Health Check did not report time synchronization issues.
* [Fixed](https://github.com/ydb-platform/ydb/pull/11658) a rare issue that caused errors during read queries.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13501) an uncommitted changes leak and cleaned them up on startup.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13948) consistency issues related to caching deleted ranges.

### Version 24.3.11.14 {#24-3-11-14}

Release date: January 9, 2025.

#### Functionality

* [Added](https://github.com/ydb-platform/ydb/pull/13251) support for restart without downtime in [a minimal fault-tolerant configuration of a cluster](./concepts/topology.md#reduced) that uses the three-node variant of `mirror-3-dc`.
* [Added](https://github.com/ydb-platform/ydb/pull/13220) new UDF Roaring Bitmap functions: AndNotWithBinary, FromUint32List, RunOptimize.

### Version 24.3.11.13 {#24-3-11-13}

Release date: December 24, 2024.

#### Functionality

* Introduced [query tracing](./reference/observability/tracing/setup), a tool that allows you to view the detailed path of a request through a distributed system.
* Added support for [asynchronous replication](./concepts/async-replication), that allows synchronizing data between YDB databases in near real time. It can also be used for data migration between databases with minimal downtime for applications interacting with these databases.
* Added support for [views](./concepts/datamodel/view), which can be enabled by the cluster administrator using the `enable_views` setting in [dynamic configuration](./maintenance/manual/dynamic-config#updating-dynamic-configuration).
* Extended [federated query](./concepts/federated_query/) capabilities to support new external data sources: MySQL, Microsoft SQL Server, and Greenplum.
* Published [documentation](./devops/manual/federated-queries/connector-deployment) on deploying YDB with [federated query](./concepts/federated_query/) functionality (manual setup).
* Added a new launch parameter `FQ_CONNECTOR_ENDPOINT` for YDB Docker containers that specifies an external data source connector address. Added support for TLS encryption for connections to the connector and the ability to expose the connector service port locally on the same host as the dynamic YDB node.
* Added an [auto-partitioning mode](./concepts/topic#autopartitioning) for topics, where partitions can dynamically split based on load while preserving message read-order and exactly-once guarantees. The mode can be enabled by the cluster administrator using the settings `enable_topic_split_merge` and `enable_pqconfig_transactions_at_scheme_shard` in [dynamic configuration](./maintenance/manual/dynamic-config#updating-dynamic-configuration).
* Added support for transactions involving [topics](./concepts/topic) and row-based tables, enabling transactional data transfer between tables and topics, or between topics, ensuring no data loss or duplication. Transactions can be enabled by the cluster administrator using the settings `enable_topic_service_tx` and `enable_pqconfig_transactions_at_scheme_shard` in [dynamic configuration](./maintenance/manual/dynamic-config#updating-dynamic-configuration).
* [Implemented](https://github.com/ydb-platform/ydb/pull/7150) [Change Data Capture (CDC)](./concepts/cdc) for synchronous secondary indexes.
* Added support for changing record retention periods in [CDC](./concepts/cdc) topics.
* Added support for auto-increment columns as part of a table's primary key.
* Added audit logging for user login events in YDB, session termination events in the user interface, and backup/restore operations.
* Added a system view with information about sessions installed from the database using a query.
* Added support literal default values for row-oriented tables. When inserting a new row in YDB Query default values will be assigned to the column if specified.
* Added the `version()` [built-in function](./yql/reference/builtins/basic.md#version).
* Added support for `RETURNING` clause in queries.
* [Added](https://github.com/ydb-platform/ydb/pull/8708) start/end times and authors in the metadata for backup/restore operations from S3-compatible storage.
* Added support for backup/restore of ACL for tables from/to S3-compatible storage.
* Included paths and decompression methods in query plans for reading from S3.
* Added new parsing options for timestamp/datetime fields when reading data from S3.
* Added support for the `Decimal` type in [partitioning keys](./dev/primary-key/column-oriented#klyuch-particionirovaniya).
* Improved diagnostics for storage issues in HealthCheck.
* **_(Experimental)_** Added a [cost-based optimizer](./concepts/optimizer#cost-based-query-optimizer) for complex queries, involving [column-oriented tables](./concepts/glossary#column-oriented-table). The cost-based optimizer considers a large number of alternative execution plans for each query and selects the best one based on the cost estimate for each option.  Currently, this optimizer only works with plans that contain [JOIN](./yql/reference/syntax/join) operations.
* **_(Experimental)_** Initial version of the workload manager was implemented. It allows to create resource pools with CPU, memory and active queries count limits. Resource classifiers were implemented to assign queries to specific resource pool.
* **_(Experimental)_** Implemented [automatic index selection](./dev/secondary-indexes#avtomaticheskoe-ispolzovanie-indeksov-pri-vyborke) for queries, which can be enabled via the `index_auto_choose_mode setting` in `table_service_config` in [dynamic configuration](./maintenance/manual/dynamic-config#updating-dynamic-configuration).

#### YDB UI

* Added support for creating and [viewing information on](https://github.com/ydb-platform/ydb-embedded-ui/issues/782) asynchronous replication instances.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/issues/929) an indicator for auto-increment columns.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1438) a tab with information about [tablets](./concepts/glossary#tablet).
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1289) a tab with details about [distributed storage groups](./concepts/glossary#storage-group).
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1218) a setting to trace all queries and display tracing results.
* Enhanced the PDisk page with [attributes](https://github.com/ydb-platform/ydb-embedded-ui/pull/1069), disk space consumption details, and a button to initiate [disk decommissioning](./devops/manual/decommissioning).
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1313) information about currently running queries.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1291) a row limit setting for query editor output and a notification when results exceed the limit.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1049) a tab to display top CPU-consuming queries over the last hour.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1127) a control to search the history and saved queries pages.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1117) the ability to cancel query execution.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/issues/944) a shortcut to save queries in the editor.
* [Separated](https://github.com/ydb-platform/ydb-embedded-ui/pull/1422) donor disks from other disks in the UI.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1154) support for InterruptInheritance ACL and improved visualization of active ACLs.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/889) a display of the current UI version.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1229) a tab  with information about the status of settings for enabling experimental functionality.

#### Performance

* [Accelerated](https://github.com/ydb-platform/ydb/pull/7589) recovery of tables with secondary indexes from backups up to 20% according to our tests.
* [Optimized](https://github.com/ydb-platform/ydb/pull/9721) Interconnect throughput.
* Improved the performance of CDC topics with thousands of partitions.
* Enhanced the Hive tablet balancing algorithm.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/6850) an issue that caused databases with a large number of tables or partitions to become non-functional during restoration from a backup. Now, if database size limits are exceeded, the restoration operation will fail, but the database will remain operational.
* [Implemented](https://github.com/ydb-platform/ydb/pull/11532) a mechanism to forcibly trigger background [compaction](./concepts/glossary#compaction) when discrepancies between the data schema and stored data are detected in [DataShard](./concepts/glossary#data-shard). This resolves a rare issue with delays in schema changes.
* [Resolved](https://github.com/ydb-platform/ydb/pull/10447) duplication of authentication tickets, which led to an increased number of requests to authentication providers.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9377) an invariant violation issue during the initial scan of CDC, leading to an abnormal termination of the `ydbd` server process.
* [Prohibited](https://github.com/ydb-platform/ydb/pull/9446) schema changes for backup tables.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9509) an issue with an initial scan freezing during CDC when the table is frequently updated.
* [Excluded](https://github.com/ydb-platform/ydb/pull/9934) deleted indexes from the count against the [maximum index limit](./concepts/limits-ydb#schema-object).
* Fixed a [bug](https://github.com/ydb-platform/ydb/issues/6985) in the display of the scheduled execution time for a set of transactions (planned step).
* [Fixed](https://github.com/ydb-platform/ydb/pull/9161) a [problem](https://github.com/ydb-platform/ydb/issues/8942) with interruptions in blue–green deployment in large clusters caused by frequent updates to the node list.
* [Resolved](https://github.com/ydb-platform/ydb/pull/8925) a rare issue that caused transaction order violations.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9841) an [issue](https://github.com/ydb-platform/ydb/issues/9797) in the EvWrite API that resulted in incorrect memory deallocation.
* [Resolved](https://github.com/ydb-platform/ydb/pull/10698) a [problem](https://github.com/ydb-platform/ydb/issues/10674) with volatile transactions hanging after a restart.
* Fixed a bug in the CDC, which in some cases leads to increased CPU consumption, up to a core per CDC partition.
* [Eliminated](https://github.com/ydb-platform/ydb/pull/11061) read delays occurring during and after the splitting of certain partitions.
* Fixed issues when reading data from S3.
* [Corrected](https://github.com/ydb-platform/ydb/pull/4793) the calculation of the AWS signature for S3 requests.
* Resolved false positives in the HealthCheck system during database backups involving a large number of shards.

## Version 24.2 {#24-2}

Release date: August 20, 2024.

### Functionality

* Added the ability to set [maintenance task priorities](./devops/manual/maintenance-without-downtime#priority) in the [cluster management system](./concepts/glossary#cms).
* Added a setting to enable [stable names](./reference/configuration/#node-broker-config) for cluster nodes within a tenant.
* Enabled retrieval of nested groups from the [LDAP server](./concepts/auth#ldap-auth-provider), improved host parsing in the [LDAP-configuration](./reference/configuration/#ldap-auth-config), and added an option to disable built-in authentication via login and password.
* Added support for authenticating [dynamic nodes](./concepts/glossary#dynamic) using SSL-certificates.
* Implemented the removal of inactive nodes from [Hive](./concepts/glossary#hive) without a restart.
* Improved management of inflight pings during Hive restarts in large clusters.
* Changed the order of establishing connections with nodes during Hive restarts.

### YDB UI

* [Added](https://github.com/ydb-platform/ydb/pull/7485) the option to set a TTL for user sessions in the configuration file.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/issues/996) an option to sort the list of queries by `CPUTime`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/7779) precision loss when working with `double`, `float` data types.
* [Added support](https://github.com/ydb-platform/ydb-embedded-ui/pull/958) for creating directories in the UI.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/976) an auto-refresh control on all pages.
* [Improved](https://github.com/ydb-platform/ydb-embedded-ui/pull/955) ACL display.
* Enabled autocomplete in the queries editor by default.
* Added support for views.

### Bug fixes

* Added a check on the size of the local transaction prior to its commit to fix [errors](https://github.com/db-platform/ydb/issues/6677) in scheme shard operations when exporting/backing up large databases.
* [Fixed](https://github.com/ydb-platform/ydb/pull/7709) an issue with duplicate results in SELECT queries when reducing quotas in [DataShard](./concepts/glossary#data-shard).
* [Fixed](https://github.com/ydb-platform/ydb/pull/6461) [errors](https://github.com/ydb-platform/ydb/issues/6220) occurring during [coordinator](./concepts/glossary#coordinator) state changes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/5992) issues during the initial CDC scan.
* [Resolved](https://github.com/ydb-platform/ydb/pull/6615) race conditions in asynchronous change delivery (asynchronous indexes, CDC).
* [Fixed](https://github.com/ydb-platform/ydb/pull/5993) a crash that sometimes occurred during [TTL-based](./concepts/ttl) deletions.
* [Fixed](https://github.com/ydb-platform/ydb/pull/5760) an issue with PDisk status display in the [CMS](./concepts/glossary#cms).
* [Fixed](https://github.com/ydb-platform/ydb/pull/6008) an issue that might cause soft tablet transfers (drain) from a node to hang.
* [Resolved](https://github.com/ydb-platform/ydb/pull/6445) an issue with the interconnect proxy stopping on a node that is running without restarts. The issue occurred when adding another node to the cluster.
* [Corrected](https://github.com/ydb-platform/ydb/pull/7023) string escaping in error messages.
* [Fixed](https://github.com/ydb-platform/ydb/pull/6695) an issue with managing free memory in the [interconnect](./concepts/glossary#actor-system-interconnect).
* [Corrected](https://github.com/ydb-platform/ydb/issues/6405) UnreplicatedPhantoms and UnreplicatedNonPhantoms counters in VDisk.
* [Fixed](https://github.com/ydb-platform/ydb/issues/6398) an issue with handling empty garbage collection requests on VDisk.
* [Resolved](https://github.com/ydb-platform/ydb/pull/5894) issues with managing TVDiskControls settings through CMS.
* [Fixed](https://github.com/ydb-platform/ydb/pull/5883) an issue with failing to load the data created by newer versions of VDisk.
* [Fixed](https://github.com/ydb-platform/ydb/pull/5862) an issue with executing the `REPLACE INTO` queries with default values.
* [Fixed](https://github.com/ydb-platform/ydb/pull/7714) errors in queries with multiple LEFT JOINs to a single string table.
* [Fixed](https://github.com/ydb-platform/ydb/pull/7740) precision loss for `float`,`double` types when using CDC.

## Version 24.1 {#24-1}

Release date: July 31, 2024.

### Functionality

* The [Knn UDF](./yql/reference/udf/list/knn.md) function for precise nearest vector search has been implemented.
* The gRPC Query service has been developed, enabling the execution of all types of queries (DML, DDL) and retrieval of unlimited amounts of data.
* [Integration with the LDAP protocol](./security/authentication.md) has been implemented, allowing the retrieval of a list of groups from external LDAP directories.

### Embedded UI

* The database information tab now includes a resource consumption diagnostic dashboard, which allows users to assess the current consumption of key resources: processor cores, RAM, and distributed storage space.
* Charts for monitoring the key performance indicators of the {{ ydb-short-name }} cluster have been added.

### Performance

* [Session timeouts](https://github.com/ydb-platform/ydb/pull/1837) for the coordination service between server and client have been optimized. Previously, the timeout was 5 seconds, which could result in a 10-second delay in identifying an unresponsive client and releasing its resources. In the new version, the check interval depends on the session's wait time, allowing for faster responses during leader changes or when acquiring distributed locks.
* CPU consumption by [SchemeShard](./concepts/glossary.md#scheme-shard) replicas has been [optimized](https://github.com/ydb-platform/ydb/pull/2391), particularly when handling rapid updates for tables with a large number of partitions.

### Bug fixes

* A possible queue overflow error has been [fixed](https://github.com/ydb-platform/ydb/pull/3917). [Change Data Capture](./dev/cdc.md) now reserves the change queue capacity during the initial scan.
* A potential deadlock between receiving and sending CDC records has been [fixed](https://github.com/ydb-platform/ydb/pull/4597).
* An issue causing the loss of the mediator task queue during mediator reconnection has been [fixed](https://github.com/ydb-platform/ydb/pull/2056). This fix allows processing of the mediator task queue during resynchronization.
* A rarely occurring error has been [fixed](https://github.com/ydb-platform/ydb/pull/2624), where with volatile transactions enabled, a successful transaction confirmation result could be returned before the transaction was fully committed. Volatile transactions remain disabled by default and are still under development.
* A rare error that led to the loss of established locks and the successful confirmation of transactions that should have failed with a "Transaction Locks Invalidated" error has been [fixed](https://github.com/ydb-platform/ydb/pull/2839).
* A rare error that could result in a violation of data integrity guarantees during concurrent read and write operations on a specific key has been [fixed](https://github.com/ydb-platform/ydb/pull/3074).
* An issue causing read replicas to stop processing requests has been [fixed](https://github.com/ydb-platform/ydb/pull/4343).
* A rare error that could cause abnormal termination of database processes if there were uncommitted transactions on a table during its renaming has been [fixed](https://github.com/ydb-platform/ydb/pull/4979).
* An error in determining the status of a static group, where it was not marked as non-working when it should have been, has been [fixed](https://github.com/ydb-platform/ydb/pull/3632).
* An error involving partial commits of a distributed transaction with uncommitted changes, caused by certain race conditions with restarts, has been [fixed](https://github.com/ydb-platform/ydb/pull/2169).
* Anomalies related to reading outdated data, [detected using Jepsen](https://blog.ydb.tech/hardening-ydb-with-jepsen-lessons-learned-e3238a7ef4f2), have been [fixed](https://github.com/ydb-platform/ydb/pull/2374).

## Version 23.4 {#23-4}

Release date: May 14, 2024.

### Performance

* [Fixed](https://github.com/ydb-platform/ydb/pull/3638) an issue of increased CPU consumption by a topic actor `PERSQUEUE_PARTITION_ACTOR`.
* [Optimized](https://github.com/ydb-platform/ydb/pull/2083) resource usage by SchemeBoard replicas. The greatest effect is noticeable when modifying the metadata of tables with a large number of partitions.

### Bug fixes

* [Fixed a bug](https://github.com/ydb-platform/ydb/pull/2169) of possible partial commit of accumulated changes when using persistent distributed transactions. This error occurs in an extremely rare combination of events, including restarting tablets that service the table partitions involved in the transaction.
* [Fixed a bug](https://github.com/ydb-platform/ydb/pull/3165) involving a race condition between the table merge and garbage collection processes, which could result in garbage collection ending with an invariant violation error, leading to an abnormal termination of the `ydbd` server process.
* [Fixed a bug](https://github.com/ydb-platform/ydb/pull/2696) in Blob Storage, where information about changes to the composition of a storage group might not be received in a timely manner by individual cluster nodes. As a result, reads and writes of data stored in the affected group could become blocked in rare cases, requiring manual intervention.
* [Fixed a bug](https://github.com/ydb-platform/ydb/pull/3002) in Blob Storage, where data storage nodes might not start despite the correct configuration. The error occurred on systems with the experimental "blob depot" feature explicitly enabled (this feature is disabled by default).
* [Fixed a bug](https://github.com/ydb-platform/ydb/pull/2475) that sometimes occurred when writing to a topic with an empty `producer_id` with turned off deduplication. It could lead to abnormal termination of the `ydbd` server process.
* [Fixed a bug](https://github.com/ydb-platform/ydb/pull/2651) that caused the `ydbd` process to crash due to an incorrect session state when writing to a topic.
* [Fixed a bug](https://github.com/ydb-platform/ydb/pull/3587) in displaying the metric of number of partitions in a topic, where it previously displayed an incorrect value.
* [Fixed a bug](https://github.com/ydb-platform/ydb/pull/2126) causing memory leaks that appeared when copying topic data between clusters. These could cause `ydbd` server processes to terminate due to out-of-memory issues.

## Version 23.3 {#23-3}

Release date: October 12, 2023.

### Functionality

* Implemented visibility of own changes. With this feature enabled you can read changed values from the current transaction, which has not been committed yet. This functionality also allows multiple modifying operations in one transaction on a table with secondary indexes.
* Added support for [column tables](concepts/datamodel/table.md#column-tables). It is now possible to create analytical reports based on stored data in YDB with performance comparable to specialized analytical DBMS.
* Added support for Kafka API for topics. YDB topics can now be accessed via a Kafka-compatible API designed for migrating existing applications. Support for Kafka protocol version 3.4.0 is provided.
* Added the ability to [write to a topic without deduplication](concepts/topic.md#no-dedup). This is important in cases where message processing order is not critical.
* YQL has added the capabilities to [create](yql/reference/syntax/create-topic.md), [modify](yql/reference/syntax/alter-topic.md), and [delete](yql/reference/syntax/delete.md) topics.
* Added support of assigning and revoking access rights using the YQL `GRANT` and `REVOKE` commands.
* Added support of DML-operations logging in the audit log.
* **_(Experimental)_** When writing messages to a topic, it is now possible to pass metadata. To enable this functionality, add `enable_topic_message_meta: true` to the [configuration file](reference/configuration/index.md).
* **_(Experimental)_** Added support for [reading from topics in a transaction](reference/ydb-sdk/topic.md#read-tx). It is now possible to read from topics and write to tables within a transaction, simplifying the data transfer scenario from a topic to a table. To enable this functionality, add `enable_topic_service_tx: true` to the [configuration file](reference/configuration/index.md).
* **_(Experimental)_** Added support for PostgreSQL compatibility. This involves executing SQL queries in PostgreSQL dialect on the YDB infrastructure using the PostgreSQL network protocol. With this capability, familiar PostgreSQL tools such as psql and drivers (e.g., pq for Golang and psycopg2 for Python) can be used. Queries can be developed using the familiar PostgreSQL syntax and take advantage of YDB's benefits such as horizontal scalability and fault tolerance.
* **_(Experimental)_** Added support for federated queries. This enables retrieving information from various data sources without the need to move the data into YDB. Federated queries support interaction with ClickHouse and PostgreSQL databases, as well as S3 class data stores (Object Storage). YQL queries can be used to access these databases without duplicating data between systems.

### Embedded UI

* A new option `PostgreSQL` has been added to the query type selector settings, which is available when the `Enable additional query modes` parameter is enabled. Also, the query history now takes into account the syntax used when executing the query.
* The YQL query template for creating a table has been updated. Added a description of the available parameters.
* Now sorting and filtering for Storage and Nodes tables takes place on the server. To use this functionality, you need to enable the parameter `Offload tables filters and sorting to backend` in the experiments section.
* Buttons for creating, changing and deleting [topics](concepts/topic.md) have been added to the context menu.
* Added sorting by criticality for all issues in the tree in `Healthcheck`.

### Performance

* Implemented read iterators. This feature allows to separate reads and computations. Read iterators allow datashards to increase read queries throughput.
* The performance of writing to YDB topics has been optimized.
* Improved tablet balancing during node overload.

### Bug fixes

* Fixed an error regarding potential blocking of reading iterators of snapshots, of which the coordinators were unaware.
* Memory leak when closing the connection in Kafka proxy has been fixed.
* Fixed an issue where snapshots taken through reading iterators may fail to recover on restarts.
* Fixed an issue with an incorrect residual predicate for the `IS NULL` condition on a column.
* Fixed an occurring verification error: `VERIFY failed: SendResult(): requirement ChunksLimiter.Take(sendBytes) failed`.
* Fixed `ALTER TABLE` for TTL on column-based tables.
* Implemented a `FeatureFlag` that allows enabling/disabling work with `CS` and `DS`.
* Fixed a 50ms time difference between coordinator time in 23-2 and 23-3.
* Fixed an error where the storage endpoint was returning extra groups when the `viewer backend` had the `node_id` parameter in the request.
* Added a usage filter to the `/storage` endpoint in the `viewer backend`.
* Fixed an issue in Storage v2 where an incorrect number was returned in the `Degraded field`.
* Fixed an issue with cancelling subscriptions from sessions during tablet restarts.
* Fixed an error where `healthcheck alerts` for storage were flickering during rolling restarts when going through a load balancer.
* Updated `CPU usage metrics` in YDB.
* Fixed an issue where `NULL` was being ignored when specifying `NOT NULL` in the table schema.
* Implemented logging of `DDL` operations in the common log.
* Implemented restriction for the YDB table attribute `add/drop` command to only work with tables and not with any other objects.
* Disabled `CloseOnIdle` for interconnect.
* Fixed the doubling of read speed in the UI.
* Fixed an issue where data could be lost on block-4-2.
* Added a check for topic name validity.
* Fixed a possible deadlock in the actor system.
* Fixed the `KqpScanArrowInChanels::AllTypesColumns` test.
* Fixed the `KqpScan::SqlInParameter` test.
* Fixed parallelism issues for OLAP queries.
* Fixed the insertion of `ClickBench` parquet files.
* Added a missing call to `CheckChangesQueueOverflow` in the general `CheckDataTxReject`.
* Fixed an error that returned an empty status in `ReadRows` API calls.
* Fixed incorrect retry behavior in the final stage of export.
* Fixed an issue with infinite quota for the number of records in a `CDC topic`.
* Fixed the import error of `string` and `parquet` columns into an `OLAP string column`.
* Fixed a crash in `KqpOlapTypes.Timestamp` under `tsan`.
* Fixed a `viewer backend` crash when attempting to execute a query against the database due to version incompatibility.
* Fixed an error where the viewer did not return a response from the `healthcheck` due to a timeout.
* Fixed an error where incorrect `ExpectedSerial` values could be saved in `Pdisks`.
* Fixed an error where database nodes were crashing due to segfault in the S3 actor.
* Fixed a race condition in `ThreadSanitizer: data race KqpService::ToDictCache-UseCache`.
* Fixed a race condition in `GetNextReadId`.
* Fixed an issue with an inflated result in `SELECT COUNT(*)` immediately after import.
* Fixed an error where `TEvScan` could return an empty dataset in the case of shard splitting.
* Added a separate `issue/error` code in case of available space exhaustion.
* Fixed a `GRPC_LIBRARY Assertion` failed error.
* Fixed an error where scanning queries on secondary indexes returned an empty result.
* Fixed validation of `CommitOffset` in `TopicAPI`.
* Reduced shared cache consumption when approaching OOM.
* Merged scheduler logic from data executer and scan executer into one class.
* Added discovery and `proxy` handlers to the query execution process in the `viewer backend`.
* Fixed an error where the `/cluster` endpoint returned the root domain name, such as `/ru`, in the `viewer backend`.
* Implemented a seamless table update scheme for `QueryService`.
* Fixed an issue where `DELETE` returned data and did not delete it.
* Fixed an error in `DELETE ON` operation in query service.
* Fixed an unexpected batching disablement in `default` schema settings.
* Fixed a triggering check `VERIFY failed: MoveUserTable(): requirement move.ReMapIndexesSize() == newTableInfo->Indexes.size()`.
* Increased the `default` timeout for `grpc-streaming`.
* Excluded unused messages and methods from `QueryService`.
* Added sorting by `Rack` in /nodes in the `viewer backend`.
* Fixed an error where sorting queries returned an error in descending order.
* Improved interaction between `KQP` and `NodeWhiteboard`.
* Removed support for old parameter formats.
* Fixed an error where `DefineBox` was not being applied to disks with a static group.
* Fixed a `SIGSEGV` error in the dinnode during `CSV` import via `YDB CLI`.
* Fixed an error that caused a crash when processing `NGRpcService::TRefreshTokenImpl`.
* Implemented a `gossip protocol` for exchanging cluster resource information.
* Fixed an error in `DeserializeValuePickleV1(): requirement data.GetTransportVersion() == (ui32) NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0 failed`.
* Implemented `auto-increment` columns.
* Use `UNAVAILABLE` status instead of `GENERIC_ERROR` when shard identification fails.
* Added support for rope payload in `TEvVGet`.
* Added ignoring of deprecated events.
* Fixed a crash of write sessions on an invalid topic name.
* Fixed an error in `CheckExpected(): requirement newConstr failed, message: Rewrite error, missing Distinct((id)) constraint in node FlatMap`.
* Enabled `self-heal` by default.

## Version 23.2 {#23-2}

Release date: August 14, 2023.

### Functionality

* **_(Experimental)_** Implemented visibility of own changes. With this feature enabled you can read changed values from the current transaction, which has not been committed yet. This functionality also allows multiple modifying operations in one transaction on a table with secondary indexes. To enable this feature add `enable_kqp_immediate_effects: true` under `table_service_config` section into [configuration file](reference/configuration/index.md).
* **_(Experimental)_** Implemented read iterators. This feature allows to separate reads and computations. Read iterators allow datashards to increase read queries throughput. To enable this feature add `enable_kqp_data_query_source_read: true` under `table_service_config` section into [configuration file](reference/configuration/index.md).

### Embedded UI

* Navigation improvements:
  * Diagnostics and Development mode switches are moved to the left panel.
  * Every page has breadcrumbs.
  * Storage groups and nodes info are moved from left buttons to tabs on the database page.
* Query history and saved queries are moved to tabs over the text editor area in query editor.
* Info tab for scheme objects displays parameters using terms from `CREATE` or `ALTER` statements.
* Added [column tables](concepts/datamodel/table.md#column-tables) support.

### Performance

* For scan queries, you can now effectively search for individual rows using a primary key or secondary indexes. This can bring you a substantial performance gain in many cases. Similarly to regular queries, you need to explicitly specify its name in the query text using the `VIEW` keyword to use a secondary index.

* **_(Experimental)_** Added an option to give control of the system tablets of the database (SchemeShard, Coordinators, Mediators, SysViewProcessor) to its own Hive instead of the root Hive, and do so immediately upon creating a new database. Without this flag, the system tablets of the new database are created in the root Hive, which can negatively impact its load. Enabling this flag makes databases completely isolated in terms of load, that may be particularly relevant for installations, consisting from a roughly hundred or more databases. To enable this feature add `alter_database_create_hive_first: true` under `feature_flags` section into [configuration file](reference/configuration/index.md).

### Bug fixes

* Fixed a bug in the autoconfiguration of the actor system, resulting in all the load being placed on the system pool.
* Fixed a bug that caused full scanning when searching by prefix of the primary key using `LIKE`.
* Fixed bugs when interacting with datashard followers.
* Fixed bugs when working with memory in column tables.
* Fixed a bug in processing conditions for immediate transactions.
* Fixed a bug in the operation of iterator-based reads on datasharrd followers.
* Fixed a bug that caused cascading reinstallation of data delivery sessions to asynchronous indexes.
* Fixed bugs in the optimizer for scanning queries.
* Fixed a bug in the incorrect calculation of storage consumption by Hive after expanding the database.
* Fixed a bug that caused operations to hang on non-existent iterators.
* Fixed bugs when reading a range on a `NOT NULL` column.
* Fixed a bug in the replication of VDisks.
* Fixed a bug in the work of the `run_interval` option in TTL.

## Version 23.1 {#23-1}

Release date: May 5, 2023. To update to version 23.1, select the [Downloads](downloads/index.md#ydb-server) section.

### Functionality

* Added [initial table scan](concepts/cdc.md#initial-scan) when creating a CDC changefeed. Now, you can export all the data existing at the time of changefeed creation.
* Added [atomic index replacement](dev/secondary-indexes.md#atomic-index-replacement). Now, you can atomically replace one pre-defined index with another. This operation is absolutely transparent for your application. Indexes are replaced seamlessly, with no downtime.
* Added the [audit log](security/audit-log.md): Event stream including data about all the operations on {{ ydb-short-name }} objects.

### Performance

* Improved formats of data exchanged between query stages. As a result, we accelerated SELECTs by 10% on parameterized queries and by up to 30% on write operations.
* Added [autoconfiguring](reference/configuration/index.md#autoconfig) for the actor system pools based on the workload against them. This improves performance through more effective CPU sharing.
* Optimized the predicate logic: Processing of parameterized OR or IN constraints is automatically delegated to DataShard.
* (Experimental) For scan queries, you can now effectively search for individual rows using a primary key or secondary indexes. This can bring you a substantial gain in performance in many cases. Similarly to regular queries, to use a secondary index, you need to explicitly specify its name in the query text using the `VIEW` keyword.
* The query's computational graph is now cached at query runtime, reducing the CPU resources needed to build the graph.

### Bug fixes

* Fixed bugs in the distributed data warehouse implementation. We strongly recommend all our users to upgrade to the latest version.
* Fixed the error that occurred on building an index on NOT NULL columns.
* Fixed statistics calculation with MVCC enabled.
* Fixed errors with backups.
* Fixed the race condition that occurred at splitting and deleting a table with SDC.

## Version 22.5 {#22-5}

Release date: March 7, 2023. To update to version **22.5**, select the [Downloads](downloads/index.md#ydb-server) section.

### What's new

* Added [changefeed configuration parameters](yql/reference/syntax/alter_table/changefeed.md) to transfer additional information about changes to a topic.
* You can now [rename tables](concepts/datamodel/table.md#rename) that have TTL enabled.
* You can now [manage the record retention period](concepts/cdc.md#retention-period).

### Bug fixes and improvements

* Fixed an error inserting 0 rows with a BulkUpsert.
* Fixed an error importing Date/DateTime columns from CSV.
* Fixed an error importing CSV data with line breaks.
* Fixed an error importing CSV data with NULL values.
* Improved Query Processing performance (by replacing WorkerActor with SessionActor).
* DataShard compaction now starts immediately after a split or merge.

## Version 22.4 {#22-4}

Release date: October 12, 2022. To update to version **22.4**, select the [Downloads](downloads/index.md#ydb-server) section.

### What's new

* {{ ydb-short-name }} Topics and Change Data Capture (CDC):

  * Introduced the new Topic API. {{ ydb-short-name }} [Topic](concepts/topic.md) is an entity for storing unstructured messages and delivering them to various subscribers.
  * Added support for the Topic API to the [{{ ydb-short-name }} CLI](reference/ydb-cli/topic-overview.md) and [SDK](reference/ydb-sdk/topic.md). The Topic API provides methods for message streaming writes and reads as well as topic management.
  * Added the ability to [capture table updates](concepts/cdc.md) and send change messages to a topic.

* SDK:

  * Added the ability to handle topics in the {{ ydb-short-name }} SDK.
  * Added official support for the database/sql driver for working with {{ ydb-short-name }} in Golang.

* Embedded UI:

  * The CDC changefeed and the secondary indexes are now displayed in the database schema hierarchy as separate objects.
  * Improved the visualization of query explain plan graphics.
  * Problem storage groups have more visibility now.
  * Various improvements based on UX research.

* Query Processing:

  * Added Query Processor 2.0, a new subsystem to execute OLTP queries with significant improvements compared to the previous version.
  * Improved write performance by up to 60%, and by up to 10% for reads.
  * Added the ability to include a NOT NULL restriction for YDB primary keys when creating tables.
  * Added support for renaming a secondary index online without shutting the service down.
  * Improved the query explain view that now also includes fields for the physical operators.

* Core:

  * For read only transactions, added consistent snapshot support that does not conflict with write transactions.
  * Added BulkUpsert support for tables with asynchronous secondary indexes.
  * Added TTL support for tables with asynchronous secondary indexes.
  * Added compression support for data export to S3.
  * Added an audit log for DDL statements.
  * Added support for authentication with static credentials.
  * Added system tables for query performance troubleshooting.
