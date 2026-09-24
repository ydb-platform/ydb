# List of changes to the Yandex Enterprise DBMS server

## Version 26.1 {#26-1}

### Version 26.1.1.ent.3 {#26-1-1-ent-3}

Release date: August 3, 2026.

The version includes all the improvements contained in build {{ ydb-short-name }} 26.1.1.22, see [change description](./changelog-server.md#26-1-1-22). In addition, the version includes the following [improvements specific to the Enterprise DBMS](#26-1-1-ent-3-extras).

#### Improvements specific to the Enterprise DBMS {#26-1-1-ent-3-extras}

The changes listed below are available in Yandex Enterprise DBMS in addition to the corresponding build {{ ydb-short-name }}:

* An optimization has been added that allows filtering rows by columns from the index before querying the main table, which reduces the number of requests to the main table when executing certain types of queries.
* A set of fixes has been implemented in index access (StreamIndexLookup) that eliminates the possibility of rare «freezing» situations in executing queries and reduces RAM consumption during query execution.
* Incorrect views can now be restored from a backup. This allows restoring backups created from a database containing such views without additional actions from the administrator.
* Support for mutual authentication via certificates (mTLS) has been added in [Kafka API](./reference/kafka-api/index.md).
* A column `TraceId` with the request trace identifier has been added to the system views `.sys/top_queries_*` and `.sys/query_sessions`.

## Version 25.4 {#25-4}

### Version 25.4.1.ent.2 {#25-4-1-ent-2}

Release date: June 17, 2026.

The version includes all the improvements contained in build {{ ydb-short-name }} 25.4.1.15, see [change description](./changelog-server.md#25-4-1-15). In addition, all [additional fixes](#25-2-1-ent-13-extras) listed below for version 25.2.1.ent.13 are included.

## Version 25.3 {#25-3}

### Version 25.3.1.ent.3 {#25-3-1-ent-3}

Release date: June 11, 2026.

The version includes all the improvements contained in build {{ ydb-short-name }} 25.3.1.27, see [change description](./changelog-server.md#25-3-1-27). In addition, all [additional fixes](#25-2-1-ent-13-extras) listed below for version 25.2.1.ent.13 are included.

## Version 25.2 {#25-2}

### Version 25.2.1.ent.13 {#25-2-1-ent-13}

Release date: June 11, 2026.

The version includes all the improvements contained in build {{ ydb-short-name }} 25.2.1.26, see [change description](./changelog-server.md#25-2-1-26). In addition, the version includes a number of additional improvements ported from the current version 26.1.

#### Additional fixes {#25-2-1-ent-13-extras}

The changes listed below were transferred from version 26.1 to the supported stable versions of the Enterprise DBMS:

* A bug has been fixed that violated the sorting order specified in the query when accessing system tables.
* A bug in the integrity check logic of the internal state has been fixed, which in rare cases could lead to a single (not massive) restart of storage nodes.
* An optimization has been added that allows filtering rows by columns from the index before querying the main table, which reduces the number of requests to the main table when executing certain types of queries.
* A set of fixes has been implemented in index access (StreamIndexLookup) that eliminates the possibility of rare «freezing» situations in executing queries and reduces RAM consumption during query execution.
* An optimization has been added that reduces memory consumption when processing queries with the TopSort operation (`SELECT ... ORDER BY x LIMIT n`).
* Support for materializing indexes during backup and restore has been added.
* TLI (Transaction Locks Invalidated) error messages are provided with an identifier or the path of the affected table in all cases.
* Lock metrics have been added to query statistics provided through system tables `.sys/query_metrics_*`.
* Incorrect views can now be restored from a backup. This allows restoring backups created from a database containing such views without additional actions from the administrator.

### Version 25.2.1.ent.4 {#25-2-1-ent-4}

Release date: February 12, 2026.

#### Functionality

* [Analytical capabilities](./concepts/analytics/index.md) are available by default: [columnar tables](./concepts/datamodel/table.md#column-oriented-tables) can be created without enabling special flags, using LZ4 compression and hash partitioning. Supported operations include a wide range of DML (UPDATE, DELETE, UPSERT, INSERT INTO ... SELECT) and CREATE TABLE AS SELECT. Integration with dbt, Apache Airflow, Jupyter, Superset and federated queries to S3 allows building end-to-end analytical pipelines in YDB.
* [Cost optimizer](./concepts/query_execution/optimizer.md) works by default for queries that use at least one columnar table, but can be forced for other queries as well. The cost optimizer improves query execution performance by calculating the optimal order and type of joins based on table statistics; supported [hints](./dev/optimization/hints.md) allow fine-tuning execution plans for complex analytical queries.
* Data [transfer](./concepts/transfer.md) — an asynchronous mechanism for transferring data from a topic to a table — has been implemented. [Creating](./yql/reference/syntax/create-transfer.md) a transfer instance, its [modification](./yql/reference/syntax/alter-transfer.md) and [deletion](./yql/reference/syntax/drop-transfer.md) is done using YQL. For a quick start, use [an example instruction](./recipes/transfer/quickstart.md).
* [Spilling](./concepts/query_execution/spilling.md) has been added, a memory management mechanism in which intermediate data generated during query execution and exceeding the available RAM of the node is temporarily offloaded to external storage. Spilling enables the execution of user queries that require processing large amounts of data exceeding the node's available memory.
* The [maximum time for executing a single query](./concepts/limits-ydb) has been increased from 30 minutes to 2 hours.
* Support for Certificate Authority (CA) and [Yandex Cloud Identity and Access Management (IAM)](https://yandex.cloud/ru/docs/iam) authentication in [asynchronous replication](./yql/reference/syntax/create-async-replication.md) has been added.
* Enabled by default:

  * [vector index](./dev/vector-indexes.md) for approximate vector search;
  * support for [YDB Topics Kafka API](./reference/kafka-api/index.md) [client balancing of readers](https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb), [compacted topics](https://docs.confluent.io/kafka/design/log_compaction.html) and [transactions](https://www.confluent.io/blog/transactions-apache-kafka);
  * support for [auto-partitioning of topics](./concepts/cdc.md#topic-partitions) in CDC for string tables;
  * support for auto-partitioning of topics for asynchronous replication;
  * support for parameterized [Decimal type](./yql/reference/types/primitive.md#numeric);
  * support for [DateTime64 type](./yql/reference/types/primitive.md#datetime);
  * automatic deletion of temporary directories and tables when exporting to S3;
  * support for [change stream](./concepts/cdc.md) in backup and restore operations;
  * the ability to [specify the number of replicas](./yql/reference/syntax/alter_table/indexes.md) for a secondary index;
  * system views with [history of overloaded partitions](./dev/system-views#top-overload-partitions).

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/24265) a bug in [Workload Manager](./dev/resource-consumption-management.md) that could cause CPU consumption by columnar tables to exceed the set limits.
* [Fixed](https://github.com/ydb-platform/ydb/pull/25112) a [problem](https://github.com/ydb-platform/ydb/issues/23858) that could cause tablet deletion to hang [](./concepts/glossary.md#tablet).
* [Fixed](https://github.com/ydb-platform/ydb/pull/25145) a [bug](https://github.com/ydb-platform/ydb/issues/20866) causing an error when changing the table follower.
* Fixed a number of bugs related to [changefeed](./concepts/glossary.md#changefeed):
  * [Fixed](https://github.com/ydb-platform/ydb/pull/25689) a [bug](https://github.com/ydb-platform/ydb/issues/25524) that could cause table import with a Utf8 key and enabled changefeed to fail.
  * [Fixed](https://github.com/ydb-platform/ydb/pull/25453) a [bug](https://github.com/ydb-platform/ydb/issues/25454) where table import without change streams could fail due to incorrect changefeed file search.
* [Fixed](https://github.com/ydb-platform/ydb/pull/26069) a [bug](https://github.com/ydb-platform/ydb/issues/25869) that could cause failures during UPSERT operations in columnar tables.
* [Fixed](https://github.com/ydb-platform/ydb/pull/26504) a [bug](https://github.com/ydb-platform/ydb/issues/26225) that caused a crash due to accessing already freed memory.
* [Fixed](https://github.com/ydb-platform/ydb/pull/26657) a [bug](https://github.com/ydb-platform/ydb/issues/23122) with duplicates in unique secondary indexes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/26879) a [bug](https://github.com/ydb-platform/ydb/issues/26565) of checksum mismatch during recovery of compressed backups from S3.
* [Fixed](https://github.com/ydb-platform/ydb/pull/27528) a [bug](https://github.com/ydb-platform/ydb/issues/27193) that could cause some TPC-H 1000 benchmark queries to fail.
* Fixed a number of issues related to cluster initialization:
  * [Fixed](https://github.com/ydb-platform/ydb/pull/25678) a [bug](https://github.com/ydb-platform/ydb/issues/25023) that could cause cluster initialization to hang with mandatory authorization.
  * [Fixed](https://github.com/ydb-platform/ydb/pull/28886) a [problem](https://github.com/ydb-platform/ydb/issues/27228) that made it impossible to create new databases immediately after cluster deployment for several minutes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/28655) a [bug](https://github.com/ydb-platform/ydb/issues/28510) where a race condition could occur and clients would receive an error `Could not find correct token validator` if recently issued tokens were used before the state was updated `LoginProvider`.

## Version 25.1 {#25-1}

### Version 25.1.4.ent.8 {#25-1-4-ent-8}

Release date: February 12, 2026.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/29940) a [bug](https://github.com/ydb-platform/ydb/issues/29903) where a named expression containing another named expression led to incorrect backup `VIEW`.
* [Fixed](https://github.com/ydb-platform/ydb/commit/c3b025603a6ba71d27ef0f1f66b9f643407643b3) a bug that caused descending sorting to work incorrectly in queries to system views.

### Version 25.1.4.ent.3 {#25-1-4-ent-3}

Release date: November 25, 2025.

#### Functionality

* [Implemented](https://github.com/ydb-platform/ydb/pull/19504) a [vector index](./dev/vector-indexes.md?version=v25.1) for approximate vector search. Recipes for [YDB CLI and YQL](./recipes/vector-search?version=v25.1) have been published for vector search, as well as examples of work [in C++ and Python](./recipes/ydb-sdk/vector-search?version=v25.1).
* [Added](https://github.com/ydb-platform/ydb/issues/11454) support for [consistent asynchronous replication](./concepts/async-replication.md?version=v25.1).
* Added a [V2 configuration mechanism](./devops/configuration-management/configuration-v2/config-overview?version=v25.1) that simplifies the deployment of new clusters {{ ydb-short-name }} and further work with them. [Comparison](./devops/configuration-management/compare-configs?version=v25.1) of V1 and V2 configuration mechanisms.
* Added support for parameterized [Decimal type](./yql/reference/types/primitive.md?version=v25.1#numeric).
* Implemented client-side partition balancing when reading via the [Kafka protocol](https://kafka.apache.org/documentation/#consumerconfigs_partition.assignment.strategy) (like Kafka itself). Previously, balancing occurred on the server. Enabled by setting a flag `enable_kafka_native_balancing` in the cluster configuration.
* Added support for [auto-partitioning of topics](./concepts/cdc.md?version=v25.1#topic-partitions) in CDC for string tables. Enabled by setting a flag `enable_topic_autopartitioning_for_cdc` in the cluster configuration.
* [Added](https://github.com/ydb-platform/ydb/pull/8264) the ability to [change the data retention time](./concepts/cdc.md?version=v25.1#topic-options) in the CDC topic using an expression `ALTER TOPIC`.
* [Supported](https://github.com/ydb-platform/ydb/pull/7052) a [format DEBEZIUM_JSON](./concepts/cdc.md?version=v25.1#debezium-json-record-structure) for change streams (changefeed).
* [Added](https://github.com/ydb-platform/ydb/pull/19507) the ability to create change streams for index tables.
* Added the ability to [specify the number of replicas](./yql/reference/syntax/alter_table/indexes.md?version=v25.1) for a secondary index. Enabled by setting a flag `enable_access_to_index_impl_tables` in the cluster configuration.
* In backup and restore operations [supported](https://github.com/ydb-platform/ydb/issues/7054) change streams. To use the functionality, you need to set flags `enable_changefeeds_export` and `enable_changefeeds_export` in the `feature_flags` section of the [database](./devops/configuration-management/configuration-v1/dynamic-config.md) or [cluster](./devops/configuration-management/configuration-v1/static-config.md) configuration.
* Added automatic deletion of temporary directories and tables when exporting to S3. Enabled by setting a flag `enable_export_auto_dropping` in the cluster configuration.
* [Added](https://github.com/ydb-platform/ydb/pull/12909) automatic integrity checking of backups during import, preventing recovery from corrupted backups and protecting against data loss.
* [Added](https://github.com/ydb-platform/ydb/pull/15570) the ability to create views using [UDF](./yql/reference/builtins/basic.md?version=v25.1#udf) in queries.
* Added system views with information about [access rights settings](./dev/system-views?version=v25.1#auth), [history of overloaded partitions](./dev/system-views?version=v25.1#top-overload-partitions) — enabled by setting a flag `enable_followers_stats` in the cluster configuration, [history of string table partitions with broken locks (TLI)](./dev/system-views?version=v25.1#top-tli-partitions).
* New parameters have been added to the [CREATE USER](./yql/reference/syntax/create-user.md?version=v25.1) and [ALTER USER](./yql/reference/syntax/alter-user.md?version=v25.1) statements:
  * `HASH` — the ability to set a password in encrypted form;
  * `LOGIN` and `NOLOGIN` — unlocking and blocking a user.
* User account security has been enhanced:
  * [Added](https://github.com/ydb-platform/ydb/pull/11963) [password complexity check](./reference/configuration/?version=v25.1#password-complexity) for the user;
  * [Implemented](https://github.com/ydb-platform/ydb/pull/12578) [automatic user blocking](./reference/configuration/?version=v25.1#account-lockout) when the password attempt limit is exceeded;
  * [Added](https://github.com/ydb-platform/ydb/pull/12983) the ability for a user to change their password independently.
* [Implemented](https://github.com/ydb-platform/ydb/issues/9748) the ability to toggle feature flags while the {{ ydb-short-name }} server is running. Flags that do not have the `(RequireRestart) = true` parameter specified in the [proto file](https://github.com/ydb-platform/ydb/blob/main/ydb/core/protos/feature_flags.proto#L60) will be applied without restarting the cluster.
* Now the oldest (rather than new) locks [are changed to full-shard](https://github.com/ydb-platform/ydb/pull/11329) when the number of locks on shards is exceeded.
* [Implemented](https://github.com/ydb-platform/ydb/pull/12567) the preservation of optimistic locks in memory during a smooth restart of data shards, which should reduce the number of ABORTED errors due to loss of locks when balancing tables between nodes.
* [Implemented](https://github.com/ydb-platform/ydb/pull/12689) the cancellation of volatile transactions with the ABORTED status during a smooth restart of data shards.
* [Added](https://github.com/ydb-platform/ydb/pull/6342) the ability to remove `NOT NULL`constraints on a column in a table using the `ALTER TABLE ... ALTER COLUMN ... DROP NOT NULL` query.
* [Added](https://github.com/ydb-platform/ydb/pull/9168) a limit of 100,000 on the number of simultaneous requests to create sessions in the coordination service.
* [Increased](https://github.com/ydb-platform/ydb/pull/14219) the maximum [number of columns in the primary key](./concepts/limits-ydb.md?version=v25.1#schema-object) from 20 to 30.
* Diagnostics and introspection of memory-related errors have been improved ([#10419](https://github.com/ydb-platform/ydb/pull/10419), [#11968](https://github.com/ydb-platform/ydb/pull/11968)).
* **_(Experimentally)_** [Added](https://github.com/ydb-platform/ydb/pull/14075) an experimental mode of operation with more stringent access rights checks. It is enabled by setting the following flags:
  * `enable_strict_acl_check` — do not allow granting rights to non-existent users and deleting users if they have been granted rights;
  * `enable_strict_user_management` — enables strict rules for administering local users (i.e., only a cluster or database administrator can administer local users);
  * `enable_database_admin` — adds a database administrator role.
* [Added](https://github.com/ydb-platform/ydb/pull/21119) the ability to use familiar tools for stream data processing — Kafka Connect, Confluent Schema Registry, Kafka Streams, Apache Flink, AKH via [Kafka API](./reference/kafka-api/index.md) when working with YDB Topics. Now YDB Topics Kafka API supports:
  * client balancing of readers — enabled by setting the `enable_kafka_native_balancing` flag in the [cluster configuration](./reference/configuration/feature_flags.md). [How reader balancing works in Apache Kafka](https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb). Now reader balancing in YDB Topics Kafka API will work the same way;
  * [compacted topics](https://docs.confluent.io/kafka/design/log_compaction.html) — enabled by setting the `enable_topic_compactification_by_key` flag;
  * [transactions](https://www.confluent.io/blog/transactions-apache-kafka) — enabled by setting the `enable_kafka_transactions` flag.
* [Added](https://github.com/ydb-platform/ydb/pull/20982) a [new protocol](https://github.com/ydb-platform/ydb/issues/11064) in [Node Broker](./concepts/glossary.md#node-broker), which eliminates spikes in network traffic on large clusters (more than 1000 servers) associated with broadcasting node information.

#### Changes that break backward compatibility

* If you use queries that refer to named expressions as tables using [AS_TABLE](./yql/reference/syntax/select/from_as_table?version=v25.1), update [temporal over YDB](https://github.com/yandex/temporal-over-ydb) to version [v1.23.0-ydb-compat](https://github.com/yandex/temporal-over-ydb/releases/tag/v1.23.0-ydb-compat) before updating YDB to the current version to avoid errors in executing such queries.

#### YDB UI

* The query editor [has added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1974) support for partial loading of results — display starts immediately upon receiving the first fragment from the server without waiting for the query to complete fully. This allows you to get results faster.
* [Improved](https://github.com/ydb-platform/ydb-embedded-ui/pull/1967) security: controls that are not available to the user are no longer displayed in the interface. Users will not encounter "Access denied" errors.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1981) search by tablet ID to the "Tablets" tab.
* Added a hotkey tip that opens with the `⌘+K` combination.
* A "Operations" tab has been added to the database page, which allows you to view a list of operations and cancel them.
* The cluster monitoring panel has been updated, and the option to collapse it has been added.
* Support for case-sensitive search has been added in the hierarchical JSON display tool.
* Examples of code for connecting to YDB SDK have been added to the top panel after selecting a database, which speeds up the development process.
* The sorting of rows in the "Queries" tab has been fixed.
* Unnecessary confirmation requests when closing the browser page in the query editor have been removed — confirmation is requested only when necessary.
* [Fixed](https://github.com/ydb-platform/ydb/pull/17839) [error](https://github.com/ydb-platform/ydb/issues/15230), due to which not all tablets were displayed on the "Tablets" tab in the diagnostics section.
* Fixed [error](https://github.com/ydb-platform/ydb/issues/18735), due to which the "Storage" tab in the database diagnostics section displayed not only storage nodes.
* Fixed [serialization error](https://github.com/ydb-platform/ydb-embedded-ui/issues/2164), which could cause a crash when opening query execution statistics.
* The logic of transitioning nodes to a critical state has been changed — a CPU pool filled to 75-99% now triggers a warning rather than a critical state.

#### Performance

* [Added](https://github.com/ydb-platform/ydb/pull/6509) support for [constant folding](https://ru.wikipedia.org/wiki/%D0%A1%D0%B2%D1%91%D1%80%D1%82%D0%BA%D0%B0_%D0%BA%D0%BE%D0%BD%D1%81%D1%82%D0%B0%D0%BD%D1%82) in the query optimizer by default, which improves query performance by calculating constant expressions at the compilation stage.
* [Added](https://github.com/ydb-platform/ydb/issues/6512) a new granular timecast protocol, which will reduce the execution time of distributed transactions (slowing down one shard will not slow down all).
* [Implemented](https://github.com/ydb-platform/ydb/issues/11561) the functionality of saving the state of data shards in memory during restarts, which allows you to preserve locks and increase the chances of successful transaction execution. This reduces the execution time of long transactions by reducing the number of retries.
* [Implemented](https://github.com/ydb-platform/ydb/pull/15255) pipelined processing of internal transactions in [Node Broker](./concepts/glossary?version=v25.1#node-broker), which has sped up the launch of dynamic nodes in the {{ ydb-short-name }} cluster.
* [Improved](https://github.com/ydb-platform/ydb/pull/15607) the resilience of Node Broker to increased load from cluster nodes.
* [Enabled](https://github.com/ydb-platform/ydb/pull/19440) by default, unloadable B-Tree indexes instead of non-unloadable SST indexes, which reduces memory consumption when storing «cold» data.
* [Optimized](https://github.com/ydb-platform/ydb/pull/15264) memory consumption by storage nodes.
* [Reduced](https://github.com/ydb-platform/ydb/pull/10969) Hive startup time by 30%.
* [Optimized](https://github.com/ydb-platform/ydb/pull/6561) the replication process in the distributed storage.
* [Optimized](https://github.com/ydb-platform/ydb/pull/9491) the size of the header for large binary objects in VDisk.
* [Reduced](https://github.com/ydb-platform/ydb/pull/15517) memory consumption by cleaning allocator pages.
* [Optimized](https://github.com/ydb-platform/ydb/pull/20197) the handling of empty inputs during JOIN operations.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/9707) an error in the [Interconnect](./concepts/glossary.md?version=v25.1#actor-system-interconnect) configuration that caused performance degradation.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13993) the «Out of memory» error when deleting very large tables by regulating the number of tablets processing the operation simultaneously.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9848) an error that occurred when specifying the same database node multiple times in the configuration for system tablets.
* [Fixed](https://github.com/ydb-platform/ydb/pull/11059) the error of long (seconds) data reading during frequent table resharding operations.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9723) the error of reading from asynchronous replicas that caused a failure.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9507) rare freezes during the initial scan of [CDC](./dev/cdc.md?version=v25.1).
* [Fixed](https://github.com/ydb-platform/ydb/pull/11483) the handling of unfinished schema transactions in datashards during system restart.
* [Fixed](https://github.com/ydb-platform/ydb/pull/10460) the error of inconsistent reading from a topic when trying to explicitly acknowledge a message read within a transaction. Now, the user will receive an error when trying to acknowledge the message.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12220) an error that caused autopartitioning to work incorrectly when working with a topic in a transaction.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12905) transaction freezes when working with topics during tablet restart.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13910) the «Key is out of range» error when importing from an S3-compatible storage.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13741) incorrect determination of the end of a metadata field in the cluster configuration.
* [Improved](https://github.com/ydb-platform/ydb/pull/16420) the construction of secondary indexes: when certain errors occur, the system retries the process instead of interrupting it.
* [Fixed](https://github.com/ydb-platform/ydb/pull/16635) an error in the execution of the expression `RETURNING` in queries `INSERT INTO` and `UPSERT INTO`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/16269) the problem of the «Drop Tablet» operation freezing in PQ tablet, especially during delays in Interconnect operation.
* [Fixed](https://github.com/ydb-platform/ydb/pull/16194) an error that occurred during [compaction](./concepts/glossary.md?version=v25.1#compaction) of VDisk.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15233) the issue where long topic reading sessions ended with «too big inflight» errors.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15515) freezing when reading a topic if at least one partition had no incoming data but was being read by multiple consumers.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18614) a rare issue of PQ tablet reboots.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18378) the issue where, after updating the cluster version, Hive subscribers were started in data centers without running database nodes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/19057) error `Failed to set up listener on port 9092 errno# 98 (Address already in use)` that occurred during version update.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18905) an error that led to a segmentation fault when executing a healthcheck request simultaneously and disabling a cluster node.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18899) a failure in [string table partitioning](./concepts/datamodel/table.md?version=v25.1#partitioning_row_table) when selecting a partitioned key from access samples containing mixed operations with the full key and key prefix (for example, exact reading or range reading).
* [Fixed](https://github.com/ydb-platform/ydb/pull/18647) [error](https://github.com/ydb-platform/ydb/issues/17885) where the index type was mistakenly determined as `GLOBAL SYNC`, although `UNIQUE` was explicitly specified in the query.
* [Fixed](https://github.com/ydb-platform/ydb/pull/16797) an error where topic autopartitioning did not work when the `max_active_partition` configuration parameter was set using the `ALTER TOPIC` expression.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18938) an error where `ydb scheme describe` returned a list of columns in a different order than they were specified when creating the table.
* [Added](https://github.com/ydb-platform/ydb/pull/21918) support in asynchronous replication for a new type of change record — `reset` records (in addition to `update` and `erase` records).
* [Fixed](https://github.com/ydb-platform/ydb/pull/21836) [error](https://github.com/ydb-platform/ydb/issues/21814) where a replication instance with an unspecified `COMMIT_INTERVAL` parameter caused the process to fail.
* [Fixed](https://github.com/ydb-platform/ydb/pull/21652) rare errors when reading from a topic during partition balancing.
* [Fixed](https://github.com/ydb-platform/ydb/pull/22455) an error where, when deleting a dedicated database, system tablets of the database could remain undeleted.
* [Fixed](https://github.com/ydb-platform/ydb/pull/22203) an error where tablets could freeze due to insufficient memory on nodes. Now, tablets will automatically start as soon as sufficient resources become available on any of the nodes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/24278) an error where, when writing Kafka messages, only the first message in the batch was saved, and the rest were ignored.

## Version 24.4 {#24-4}

### Version 24.4.4.20 {#24-4-4-20}

Release date: November 1, 2025.

#### Functionality

* In backup and restore operations, [support](https://github.com/ydb-platform/ydb/pull/25675) for views (VIEW) has been added. To use this functionality, you need to set the `enable_view_export` flag in the `feature_flags` section of the [database](./devops/configuration-management/configuration-v1/dynamic-config.md) or [cluster](./devops/configuration-management/configuration-v1/static-config.md) configuration.
* Additional identifiers are added to the [Transaction locks invalidated](./troubleshooting/performance/queries/transaction-lock-invalidation) error message in case the table cannot be identified (Unknown table): the object path identifier (`PathId`) and the tablet identifier (`TabletId`).

### Version 24.4.4.15 {#24-4-4-15}

Release date: September 19, 2025.

#### Performance

* The columns by which the query results are sorted are taken into account by the optimizer when automatically selecting a secondary index. The functionality works only for queries to a single table, without joining other tables.

#### Bug fixes

* When receiving error `OperationAborted` in the response from S3, the export operation does not fail, but retries writing to S3.

### Version 24.4.4.13 {#24-4-4-13}

Release date: July 29, 2025.

#### Functionality

* [Support](https://github.com/ydb-platform/ydb/pull/11276) for restart without losing cluster availability in [the minimum fault-tolerant configuration](./concepts/topology#reduced) of three nodes.
* [Added](https://github.com/ydb-platform/ydb/pull/13218) new UDF Roaring bitmap functions: AndNotWithBinary, FromUint32List, RunOptimize.
* Added the ability to register [a database node](./concepts/glossary.md#database-node) using a certificate. The [Node Broker](./concepts/glossary.md#node-broker) has been added with a flag `AuthorizeByCertificate`to use a certificate during registration.
* [Added](https://github.com/ydb-platform/ydb/pull/11775) priorities for authenticating ticket verification [using a third-party IAM provider](./security/authentication.md#iam), with the highest priority given to requests from new users. Tickets in the cache update their information with a lower priority.
* Added the ability to [read and write to a topic](./reference/kafka-api/examples.md#primery-raboty-s-kafka-api) using the Kafka API without authentication.
* The following are enabled by default:

  * support for [views (VIEW)](./concepts/datamodel/view.md);
  * [auto-partitioning](./concepts/datamodel/topic.md#autopartitioning) mode for topics;
  * [transactions involving topics and string tables](./concepts/transactions.md#topic-table-transactions);
  * [volatile distributed transactions](./contributor/datashard-distributed-txs.md#osobennosti-vypolneniya-volatilnyh-tranzakcij).

#### Performance

* [Accelerated](https://github.com/ydb-platform/ydb/pull/12747) the startup of tablets on large clusters: 210 ms **→** 125 ms (SSD), 260 ms **→** 165 ms (HDD).
* [Limited](https://github.com/ydb-platform/ydb/pull/17755) the number of configuration changes being processed simultaneously.
* [Optimized](https://github.com/ydb-platform/ydb/issues/18289) memory consumption by PQ tablets.
* [Optimized](https://github.com/ydb-platform/ydb/issues/18473) CPU consumption by the Scheme shard tablet, which reduced the latency of responses to queries. The limit on the number of Scheme shard operations is now checked before performing split and merge operations on tablets.
* [Automatic selection of a secondary index](./dev/secondary-indexes.md#avtomaticheskoe-ispolzovanie-indeksov-pri-vyborke) is enabled by default when executing a query.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/12221) an error that caused a significant increase in CPU load when reading small messages from a topic in small batches. This could lead to delays in reading/writing to this topic.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13918) an error in restoring from a backup that was created during automatic table partitioning.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12601) an error in serialization `Uuid`for [CDC](./concepts/cdc.md).
* [Fixed](https://github.com/ydb-platform/ydb/pull/12804) an error that could cause reading on tablet subscribers to fail during automatic table partitioning.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12807) an error where the [coordination node](./concepts/datamodel/coordination-node.md) successfully registered proxy servers despite a communication breakdown.
* [Fixed](https://github.com/ydb-platform/ydb/pull/11593) an error occurring when opening a tab with information about [distributed storage groups](./concepts/glossary.md#storage-group) in the interface.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12448) [an error](https://github.com/ydb-platform/ydb/issues/12443) where [Health Check](./reference/ydb-sdk/health-check-api) did not report time synchronization issues.
* [Fixed](https://github.com/ydb-platform/ydb/pull/17123) a rare error where client applications would freeze during transaction commit when a partition was deleted before the write quota for the topic was updated.
* [Fixed](https://github.com/ydb-platform/ydb/pull/17312) an error in copying tables with the Decimal type, which caused a failure when rolling back to a previous version.
* [Fixed](https://github.com/ydb-platform/ydb/pull/17519) [an error](https://github.com/ydb-platform/ydb/issues/17499) where a commit without confirmation of writing to the topic led to the blocking of the current and subsequent transactions with topics.
* Fixed transaction freezes when working with topics during [reboot](https://github.com/ydb-platform/ydb/issues/17843) or [deletion](https://github.com/ydb-platform/ydb/issues/17915) of a tablet.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18114) [issues](https://github.com/ydb-platform/ydb/issues/18071) with reading messages larger than 6Mb via [Kafka API](./reference/kafka-api).
* [Fixed](https://github.com/ydb-platform/ydb/pull/18319) a memory leak during writing to [the topic](./concepts/glossary#topic).
* Fixed errors in processing [nullable columns](https://github.com/ydb-platform/ydb/issues/15701) and [columns with the UUID type](https://github.com/ydb-platform/ydb/issues/15697) in string tables.
* [Fixed](https://github.com/ydb-platform/ydb/pull/14811) an error that significantly reduced the reading speed from [tablet subscribers](./concepts/glossary.md#tablet-follower).
* [Fixed](https://github.com/ydb-platform/ydb/pull/14516) an error that caused the volatile distributed transaction to wait for confirmation until the next restart.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15077) a rare error that caused a failure when connecting tablet subscribers to a leader with an inconsistent command log state.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15074) a rare error that caused a failure when restarting a remote datashard with inconsistent changes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15194) an error that could disrupt the order of message processing in the topic.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15308) a rare error that could cause reading from the topic to freeze.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15160) an issue where a transaction would freeze when a user was simultaneously managing a topic and moving a PQ tablet to another node.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15233) an issue with the leakage of the counter value for userInfo, which could lead to a reading error `too big in flight`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15467) a proxy server crash due to duplicate topics in the request.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15933) a rare error that allowed a user to write to a topic bypassing account quota restrictions.
* [Fixed](https://github.com/ydb-platform/ydb/pull/16288) an issue where, after deleting a topic, the system returned "OK", but the tablets continued to operate. To delete such tablets, use the instructions from [pull request](https://github.com/ydb-platform/ydb/pull/16288).
* [Fixed](https://github.com/ydb-platform/ydb/pull/16418) a rare error that prevented the restoration of a backup of a large table with a secondary index.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15862) the issue that led to an error when inserting data using `UPSERT` into string tables with default values.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15334) the error that caused a failure when executing queries to tables with secondary indexes that return result lists using the `RETURNING *` expression.

## Version 24.3 {#24-3}

### Version 24.3.13.11 {#24-3-13-11}

Release date: March 6, 2025.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/13501) a rare issue that led to leaks of uncommitted changes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13948) consistency issues related to caching of deleted ranges.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15182) the issue of long-term caching of negative responses to authentication requests from an LDAP-compatible user and group directory.

### Version 24.3.13.10 {#24-3-13-10}

Release date: December 24, 2024.

#### Functionality

* Added [query tracing](./reference/observability/tracing/setup) — a tool that allows you to view in detail the path a query takes through a distributed system.
* Added support for [asynchronous replication](./concepts/async-replication), which allows you to synchronize data between YDB databases almost in real time. It can also be used to migrate data between databases with minimal downtime for applications working with them.
* Added support for [views (VIEW)](https://ydb.tech/docs/ru/concepts/datamodel/view), which can be enabled by the cluster administrator using the `enable_views` setting in the [dynamic configuration](./devops/configuration-management/configuration-v1/dynamic-config#obnovlenie-dinamicheskoj-konfiguracii).
* [Federated queries](./concepts/query_execution/federated_query/) now support new external data sources: MySQL, Microsoft SQL Server, Greenplum.
* Developed [documentation](./devops/deployment-options/manual/federated-queries/connector-deployment) on deploying YDB with federated query functionality (manually).
* For the YDB Docker container, added a startup parameter `FQ_CONNECTOR_ENDPOINT` that allows you to specify the address of the connector to external data sources. Added the ability to TLS-encrypt the connection to the connector. Added the ability to output the port of the connector service running locally on the same host as the YDB dynamic node.
* Added the [auto-partitioning](./concepts/datamodel/topic#autopartitioning) mode for topics, in which topics can split partitions depending on the load while maintaining guarantees of message read order and exactly once writing. The mode can be enabled by the cluster administrator using the `enable_topic_split_merge` and `enable_pqconfig_transactions_at_scheme_shard` settings in the [dynamic configuration](./devops/configuration-management/configuration-v1/dynamic-config#obnovlenie-dinamicheskoj-konfiguracii).
* Added [transactions](./concepts/transactions#topic-table-transactions) involving [topics](https://ydb.tech/docs/ru/concepts/datamodel/topic) and string tables. This allows you to transactionally move data from tables to topics and vice versa, as well as between topics, so that data is not lost or duplicated. Transactions can be enabled by the cluster administrator using the `enable_topic_service_tx` and `enable_pqconfig_transactions_at_scheme_shard` settings in the [dynamic configuration](./devops/configuration-management/configuration-v1/dynamic-config#obnovlenie-dinamicheskoj-konfiguracii).
* [Added](https://github.com/ydb-platform/ydb/pull/7150) support for [CDC](./concepts/cdc) for synchronous secondary indexes.
* Added the ability to change the retention period for records in [CDC](./concepts/cdc.md) topics.
* Added support for [auto-increment](./yql/reference/types/serial) for columns included in the primary key of a table.
* Added logging to the [audit log](./security/audit-log) of user login events in YDB, user session termination events in the user interface, as well as backup and restore requests.
* Added a system view that allows you to get information about sessions established with the database using a query.
* Added support for default constant values for columns in string tables.
* Added support for the `RETURNING` expression in queries.
* Added the [built-in function](./yql/reference/builtins/basic.md#version) `version()`.
* [Added](https://github.com/ydb-platform/ydb/pull/8708) the start/end time and author to the metadata of backup/restore operations from an S3-compatible storage.
* Added support for backing up/restoring ACL for tables from an S3-compatible storage.
* For queries reading from S3, paths and decompression method have been added to the plan.
* Added new parsing settings for `timestamp`, `datetime` when reading data from S3.
* Added support for the `Decimal` type in [partitioning keys](https://ydb.tech/docs/ru/dev/primary-key/column-oriented#klyuch-particionirovaniya).
* Improved diagnosis of storage issues in HealthCheck.
* **_(Experimentally)_** Added a [cost optimizer](./concepts/query_execution/optimizer#stoimostnoj-optimizator-zaprosov) for complex queries involving [columnar tables](./concepts/glossary#column-oriented-table). The optimizer considers a large number of alternative execution plans and selects the best one based on the cost estimate of each option. Currently, the optimizer works only with plans that include [JOIN](./yql/reference/syntax/join) operations.
* **_(Experimentally)_** Implemented an initial version of the [workload manager](./dev/resource-consumption-management), which allows you to create resource pools with limits on CPU, memory, and the number of active queries. Resource classifiers have been implemented to assign queries to a specific resource pool.
* **_(Experimentally)_** Implemented [automatic index selection](https://ydb.tech/docs/ru/dev/secondary-indexes#avtomaticheskoe-ispolzovanie-indeksov-pri-vyborke) when executing a query, which can be enabled by the cluster administrator using the `index_auto_choose_mode` setting in `table_service_config` in the [dynamic configuration](./devops/configuration-management/configuration-v1/dynamic-config#obnovlenie-dinamicheskoj-konfiguracii).

#### YDB UI

* Supported creating and [displaying](https://github.com/ydb-platform/ydb-embedded-ui/issues/782) an asynchronous replication instance.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/issues/929) the designation for [auto-increment columns](./yql/reference/types/serial).
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1438) a tab with information about [tablets](./concepts/glossary#tablet).
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1289) a tab with information about [distributed storage groups](./concepts/glossary#storage-group).
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1218) a setting to add [tracing](./reference/observability/tracing/setup) to all queries and display the tracing results of a query.
* The PDisk page has been added with [attributes](https://github.com/ydb-platform/ydb-embedded-ui/pull/1069), information about disk space consumption, and a button that triggers [disk decommissioning](./devops/deployment-options/manual/decommissioning).
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1313) information about running queries.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1291) the setting for the row limit in the query editor output and display if the query results exceed the limit.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1049) display of a list of queries with the highest CPU consumption over the last hour.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1127) search on pages with query history and a list of saved queries.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1117) the ability to interrupt query execution.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/issues/944) the ability to save a query from the editor using hotkeys.
* [Separated](https://github.com/ydb-platform/ydb-embedded-ui/pull/1422) the display of disks from donor disks.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1154) support for InterruptInheritance ACL and improved display of active ACLs.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/889) display of the current version of the user interface.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1229) information about the state of experimental functionality enablement settings.

#### Performance

* [Accelerated](https://github.com/ydb-platform/ydb/pull/7589) recovery from backup of tables with secondary indexes by up to 20% according to our tests.
* [Optimized](https://github.com/ydb-platform/ydb/pull/9721) the throughput of Interconnect.
* Improved the performance of CDC topics containing thousands of partitions.
* Made a number of improvements to the Hive tablet balancing algorithm.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/6850) an error that rendered a database with a large number of tables or partitions inoperable when restoring from a backup. Now, if the database size limits are exceeded, the restore operation will fail, and the database will continue to operate normally.
* [Implemented](https://github.com/ydb-platform/ydb/pull/11532) a mechanism that forcibly triggers background [compaction](./concepts/glossary#compaction) when discrepancies are detected between the data schema and the data stored in [DataShard](./concepts/glossary#data-shard). This resolves a rarely occurring problem with delays in changing the data schema.
* [Eliminated](https://github.com/ydb-platform/ydb/pull/10447) duplication of authentication tickets, which led to an increased number of requests to authentication providers.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9377) an error violating the invariant during the initial CDC scan, which caused the ydbd server process to crash.
* [Prohibited](https://github.com/ydb-platform/ydb/pull/9446) changing the schema of backup tables.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9509) the hanging of the initial CDC scan during frequent table updates.
* [Excluded](https://github.com/ydb-platform/ydb/pull/9934) deleted indexes from the count of the [maximum number of indexes](https://ydb.tech/docs/ru/concepts/limits-ydb#schema-object).
* [Fixed](https://github.com/ydb-platform/ydb/pull/8847) [an error](https://github.com/ydb-platform/ydb/issues/6985) in displaying the time at which the execution of a set of transactions is scheduled (planned step).
* [Fixed](https://github.com/ydb-platform/ydb/pull/9161) [an issue](https://github.com/ydb-platform/ydb/issues/8942) with interrupting blue-green deployment in large clusters due to frequent updates of the node list.
* [Fixed](https://github.com/ydb-platform/ydb/pull/8925) a rarely occurring error that led to a violation of the order of transaction execution.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9841) [an error](https://github.com/ydb-platform/ydb/issues/9797) in the EvWrite API that led to incorrect memory release.
* [Fixed](https://github.com/ydb-platform/ydb/pull/10698) [an issue](https://github.com/ydb-platform/ydb/issues/10674) with the hanging of volatile transactions after restart.
* Fixed an error in CDC that in some cases led to increased CPU usage, up to a core per CDC partition.
* [Eliminated](https://github.com/ydb-platform/ydb/pull/11061) the read delay occurring during and after the splitting of some partitions.
* Fixed errors when reading data from S3.
* [Fixed](https://github.com/ydb-platform/ydb/pull/4793) the method of calculating the aws signature when accessing S3.
* Fixed false positives of the HealthCheck system during the backup of a database with a large number of shards.
* [Removed](https://github.com/ydb-platform/ydb/pull/11901) the restriction on writing values greater than 127 to the Uint8 type.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12915) an error in restoring from a backup saved in an S3 storage with Path-style addressing.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12018) a potential failure of [“frozen” locks](./contributor/datashard-locks-and-change-visibility#vzaimodejstvie-s-raspredelyonnymi-tranzakciyami), which could be caused by mass operations (for example, deletion by TTL).
* [Fixed](https://github.com/ydb-platform/ydb/pull/11658) a rare issue that led to errors when executing a read query.

## Version 24.2 {#24-2}

### Version 24.2.7.1 {#24-2-7-1}

Release date: August 20, 2024.

### Functionality

* Added the ability to [set priorities](./devops/deployment-options/manual/maintenance.md#rolling-restart) for maintenance tasks in the [cluster management system](./concepts/glossary.md#cms).
* Added [stable names](reference/configuration/node_broker_config.md#node-broker-config) configuration for cluster nodes within a tenant.
* Added retrieval of nested groups from the [LDAP server](./security/authentication.md#ldap), improved parsing of hosts in the [LDAP configuration](reference/configuration/auth_config.md#ldap-auth-config), and added an option to disable built-in login and password authentication.
* Added the ability to authenticate [dynamic nodes](./concepts/glossary.md#dynamic) using an SSL certificate.
* Implemented the removal of inactive nodes from [Hive](./concepts/glossary.md#hive) without restarting it.
* Improved management of inflight pings when restarting Hive in large clusters.
* [Changed](https://github.com/ydb-platform/ydb/pull/6381) the order of establishing connections with nodes when restarting Hive.

### YDB UI

* [Added](https://github.com/ydb-platform/ydb/pull/7485) the ability to set a TTL for a user session in the configuration file.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1028) sorting by `CPUTime` in the table with the list of queries.
* [Fixed](https://github.com/ydb-platform/ydb/pull/7779) loss of precision when working with `double`, `float`.
* Supported [creating directories from the UI](https://github.com/ydb-platform/ydb-embedded-ui/issues/958).
* [Added the ability](https://github.com/ydb-platform/ydb-embedded-ui/pull/976) to set an interval for background data updates on all pages.
* [Improved](https://github.com/ydb-platform/ydb-embedded-ui/issues/955) ACL display.
* Enabled autocomplete in the query editor by default.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/834) support for View.

### Bug fixes

* Added a check on the size of a local transaction before its commit to fix [errors](https://github.com/ydb-platform/ydb/issues/6677) in the operation of schema operations when exporting/backing up large databases.
* [Fixed](https://github.com/ydb-platform/ydb/pull/7709) [an error](https://github.com/ydb-platform/ydb/issues/7674) of duplicate results of a SELECT query when reducing the quota in [DataShard](./concepts/glossary#data-shard).
* [Fixed](https://github.com/ydb-platform/ydb/pull/6461) [errors](https://github.com/ydb-platform/ydb/issues/6220) occurring when changing the state of the [coordinator](./concepts/glossary#coordinator).
* [Fixed](https://github.com/ydb-platform/ydb/pull/5992) errors occurring during the initial scan of [CDC](./dev/cdc).
* [Fixed](https://github.com/ydb-platform/ydb/pull/6615) a race condition in asynchronous change delivery (asynchronous indexes, CDC).
* [Fixed](https://github.com/ydb-platform/ydb/pull/5993) a rare error that caused the process to crash when deleting by [TTL](./concepts/ttl).
* [Fixed](https://github.com/ydb-platform/ydb/pull/5760) an error in displaying the PDisk status in the [CMS](./concepts/glossary#cms) interface.
* [Fixed](https://github.com/ydb-platform/ydb/pull/6008) errors that could cause the soft drain (drain) of tablets from a node to hang.
* [Fixed](https://github.com/ydb-platform/ydb/pull/6445) an error stopping the interconnect proxy on a node running without restarts when adding another node to the cluster.
* [Fixed](https://github.com/ydb-platform/ydb/pull/6695) the accounting of free memory in [interconnect](./concepts/glossary#actor-system-interconnect).
* [Fixed](https://github.com/ydb-platform/ydb/issues/6405) the counters for UnreplicatedPhantoms/UnreplicatedNonPhantoms in VDisk.
* [Fixed](https://github.com/ydb-platform/ydb/issues/6398) the handling of empty garbage collection requests on VDisk.
* [Fixed](https://github.com/ydb-platform/ydb/pull/5894) the management of TVDiskControls settings via CMS.
* [Fixed](https://github.com/ydb-platform/ydb/pull/5883) an error loading data created by newer versions of VDisk.
* [Fixed](https://github.com/ydb-platform/ydb/pull/5862) an error executing a query `REPLACE INTO` with a default value.
* [Fixed](https://github.com/ydb-platform/ydb/pull/7714) an error executing queries that performed multiple left joins to the same string table.
* [Fixed](https://github.com/ydb-platform/ydb/pull/7740) loss of precision for `float`, `double` types when using CDC.

## Version 24.1 {#24-1}

### Version 24.1.18.1 {#24-1-18-1}

Release date: July 31, 2024.

### Functionality

* Implemented [Knn UDF](./yql/reference/udf/list/knn.md) for precise search of the nearest vectors.
* Developed a gRPC QueryService that allows executing all types of queries (DML, DDL) and retrieving unlimited amounts of data.
* Implemented [integration with the LDAP protocol](./security/authentication.md) and the ability to retrieve a list of groups from external LDAP directories.

### Built-in UI

* Added a resource consumption diagnostics dashboard, located on the database information tab, which helps determine the current state of resource consumption: CPU cores, RAM, and network distributed storage space.
* Added graphs for monitoring the main cluster performance indicators {{ ydb-short-name }}.

### Performance

* [Optimized](https://github.com/ydb-platform/ydb/pull/1837) session timeouts for the coordination service from server to client. Previously, the timeout was 5 seconds, which in the worst case led to identifying a non-working client (and releasing the resources it held) within 10 seconds. In the new version, the check time depends on the session wait time, which ensures faster response when changing the leader or acquiring distributed locks.
* [Optimized](https://github.com/ydb-platform/ydb/pull/2391) CPU consumption by [SchemeShard](./concepts/glossary.md#scheme-shard) replicas, especially when processing fast updates for tables with a large number of partitions.

### Error fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/3917) a potential queue overflow error; [Change Data Capture](./dev/cdc.md) reserves the capacity of the change queue during the initial scan.
* [Fixed](https://github.com/ydb-platform/ydb/pull/4597) a potential deadlock between retrieving CDC records and sending them.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2056) the issue of losing the mediator task queue during mediator reconnection; the fix allows processing the mediator task queue during resynchronization.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2624) a rare error where, with enabled and used volatile transactions, a successful transaction confirmation result was returned before the transaction was successfully committed. Volatile transactions are disabled by default and are under development.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2839) a rare error that led to the loss of established locks and the successful confirmation of transactions that should have resulted in a Transaction Locks Invalidated error.
* [Fixed](https://github.com/ydb-platform/ydb/pull/3074) a rare error that could lead to a violation of data integrity guarantees during concurrent write and read operations on a specific key.
* [Fixed](https://github.com/ydb-platform/ydb/pull/4343) an issue that caused read replicas to stop processing requests.
* [Fixed](https://github.com/ydb-platform/ydb/pull/4979) a rare error that could lead to the database processes crashing when there were uncommitted transactions on a table at the time of its renaming.
* [Fixed](https://github.com/ydb-platform/ydb/pull/3632) an error in the logic for determining the status of a static group, where the static group was not marked as non-working when it should have been.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2169) an error of partial commit of a distributed transaction with uncommitted changes in case of some races with restarts.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2374) anomalies with reading outdated data that were [detected using Jepsen](https://blog.ydb.tech/hardening-ydb-with-jepsen-lessons-learned-e3238a7ef4f2).

## Version 23.4 {#23-4}

### Version 23.4.11.1 {#23-4-11-1}

Release date: May 14, 2024.

### Performance

* [Fixed](https://github.com/ydb-platform/ydb/pull/3638) the issue of increased consumption of computing resources by the topic actor `PERSQUEUE_PARTITION_ACTOR`.
* [Optimized](https://github.com/ydb-platform/ydb/pull/2083) resource usage by SchemeBoard replicas. The greatest effect is noticeable when modifying table metadata with a large number of partitions.

### Error fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/2169) an error of possible incomplete commit of accumulated changes when using distributed transactions. This error occurs with an extremely rare combination of events, including the restart of tablets serving the table partitions involved in the transaction.
* [Eliminated](https://github.com/ydb-platform/ydb/pull/3165) a race between table merge processes and garbage collection, which could cause garbage collection to fail with an invariant violation error and, as a result, crash the server process `ydbd`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2696) an error in Blob Storage where information about changes in the storage group composition might not be delivered in a timely manner to individual cluster nodes. As a result, in rare cases, read and write operations on data stored in the affected group could be blocked, requiring manual administrator intervention.
* [Fixed](https://github.com/ydb-platform/ydb/pull/3002) an error in Blob Storage that could prevent data storage nodes from starting with a correct configuration. The error occurred in systems with the experimental "blob depot" feature explicitly enabled (this feature is disabled by default).
* [Fixed](https://github.com/ydb-platform/ydb/pull/2475) an error that occurred in some situations when writing to a topic with an empty `producer_id` when deduplication was disabled. It could cause the server process `ydbd` to crash.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2651) an issue that caused the `ydbd` process to crash due to an erroneous write session state in the topic.
* [Fixed](https://github.com/ydb-platform/ydb/pull/3587) a display error of the metric for the number of partitions in the topic; it previously showed an incorrect value.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2126) memory leaks that occurred when copying topic data between clusters {{ ydb-short-name }}. They could cause server processes `ydbd` to terminate due to running out of available RAM.

## Version 23.3 {#23-3}

### Version 23.3.25.2 {#23-3-25-2}

Release date: October 12, 2023.

### Functionality

* Visibility of own changes within transactions has been implemented. Previously, when attempting to read data already modified in the current transaction, the query would result in an error. This required ordering reads and writes within the transaction. With the introduction of visibility of own changes, these restrictions are lifted, and queries can read rows modified in the current transaction.
* Support for [columnar tables](concepts/datamodel/table.md#column-tables) has been added. Columnar tables are well-suited for analytical queries (Online Analytical Processing) because only the columns directly involved in the query are read. YDB columnar tables allow creating analytical reports with performance comparable to specialized analytical DBMS.
* Support for [Kafka API for topics](reference/kafka-api/index.md) has been added. Now YDB topics can be accessed via a Kafka-compatible API designed for migrating existing applications. Support for the Kafka protocol version 3.4.0 is provided.
* The ability to [write to a topic without deduplication](concepts/datamodel/topic.md#no-dedup) has been added. This type of writing is well-suited for cases where the order of message processing is not critical. Writing without deduplication is faster and consumes fewer server resources, but message ordering and deduplication do not occur on the server.
* YQL has been enhanced with the ability to [create](yql/reference/syntax/create-topic.md), [modify](yql/reference/syntax/alter-topic.md), and [delete](yql/reference/syntax/drop-topic.md) topics.
* The ability to grant and revoke access rights using YQL commands [GRANT](yql/reference/syntax/grant.md) and [REVOKE](yql/reference/syntax/revoke.md) has been added.
* The ability to log DML operations in the audit log has been added.
* **_(Experimentally)_** When writing messages to a topic, it is now possible to pass metadata. To enable this functionality, add `enable_topic_message_meta: true` to the [configuration file](reference/configuration/index.md).
* **_(Experimentally)_** The ability to [read from topics](reference/ydb-sdk/topic.md#read-tx) and write to a table within a single transaction has been added. This new feature simplifies the scenario of transferring data from a topic to a table. To enable it, add `enable_topic_service_tx: true` to the configuration file.
* **_(Experimentally)_** Support for PostgreSQL compatibility has been added. The new mechanism allows executing SQL queries in the PostgreSQL dialect on the YDB infrastructure using the PostgreSQL network protocol. You can use familiar PostgreSQL tools, such as psql and drivers (pq for Golang and psycopg2 for Python), and develop queries using the familiar PostgreSQL syntax with the horizontal scalability and fault tolerance of YDB.
* **_(Experimentally)_** Support for [federated queries](concepts/query_execution/federated_query/index.md) has been added. It allows retrieving information from various data sources without moving them to YDB. Interaction with ClickHouse, PostgreSQL, and S3 is supported via YQL queries without duplicating data between systems.

### Built-in UI

* A new option `PostgreSQL` has been added to the request type selector settings, which is available when the `Enable additional query modes` parameter is enabled. The request history now takes into account the syntax used when executing the request.
* The YQL query template for creating a table has been updated. A description of the available parameters has been added.
* Sorting and filtering for Storage and Nodes tables have been moved to the server. You need to enable the `Offload tables filters and sorting to backend` parameter in the experiments section to use this functionality.
* Buttons for creating, modifying, and deleting [topics](concepts/datamodel/topic.md) have been added to the context menu.
* Sorting by severity for all issues in the tree has been added in `Healthcheck`.

### Performance

* Iterator reads have been implemented. The new functionality allows separating reads and computations. Iterator reads enable date shards to increase the throughput of read queries.
* YDB topic writing performance has been optimized.
* Tablet balancing has been improved when nodes are overloaded.

### Bug fixes

* Fixed an issue with possible blocking of snapshots by reading iterators that coordinators are not aware of.
* Fixed a memory leak when closing a connection in the kafka proxy.
* Fixed an issue where snapshots taken via reading iterators may not be restored on restarts.
* Fixed an incorrect residual predicate for the condition `IS NULL` on the column.
* Fixed the triggered check `VERIFY failed: SendResult(): requirement ChunksLimiter.Take(sendBytes) failed`.
* Fixed `ALTER TABLE` for columnar tables by `TTL`.
* Implemented `FeatureFlag`, which allows enabling/disabling work with `CS` and `DS`.
* Fixed the discrepancy in coordinator time between 23-2 and 23-3 by 50 ms.
* Fixed an error where the handle `storage` returned extra groups when the parameter `node_id` was in `viewer backend` in the query.
* Added a `usage` filter to `/storage` in `viewer backend`.
* Fixed an error in Storage v2 where an incorrect number was returned in `Degraded`.
* Fixed the issue of subscription cancellation from sessions in iterator reads on tablet restart.
* Fixed an error where during a rolling restart, when going through the balancer, `healthcheck` alerts about storage flicker.
* Updated `cpu usage` metrics in ydb.
* Fixed ignoring `NULL` when specifying `NOT NULL` in the table schema.
* Implemented output of `DDL` operation records to the general log.
* Implemented a ban for the `ydb table attribute add/drop` command to work with any objects other than tables.
* Disabled `CloseOnIdle` for `interconnect`.
* Fixed the doubling of reading speed in the UI.
* Fixed an error where data could be lost on `block-4-2`.
* Added a topic name check.
* Fixed a possible `deadlock` in the actor system.
* Fixed the test `KqpScanArrowInChanels::AllTypesColumns`.
* Fixed the test `KqpScan::SqlInParameter`.
* Fixed concurrency issues for OLAP queries.
* Fixed the insertion of `ClickBench parquet`.
* Added a missing call to `CheckChangesQueueOverflow` in the general `CheckDataTxReject`.
* Fixed the error of returning an empty status when calling `ReadRows API`.
* Fixed incorrect retry of export in the final stage.
* Fixed the issue with an infinite quota on the number of records in the CDC topic.
* Fixed the error of importing columns `string` and `parquet` into the `string` OLAP column.
* Fixed the crash of `KqpOlapTypes.Timestamp` under tsan.
* Fixed the crash in `viewer backend` when trying to execute a query to the database due to version incompatibility.
* Fixed an error where `viewer` did not return a response from `healthcheck` due to a timeout.
* Fixed an error where an incorrect value of `ExpectedSerial` could be stored in Pdisks.
* Fixed an error where database nodes crashed due to `segfault` in the S3 actor.
* Fixed a race in `ThreadSanitizer: data race KqpService::ToDictCache-UseCache`.
* Fixed a race in `GetNextReadId`.
* Fixed the overestimation of the `SELECT COUNT(*)` result immediately after import.
* Fixed an error where `TEvScan` could return an empty dataset in the case of a dateshard split.
* Added a separate issue/error code in case of exhaustion of available space.
* Fixed error `GRPC_LIBRARY Assertion failed`.
* Fixed an error where scanning queries with a secondary index returned an empty result.
* Fixed validation of `CommitOffset` in `TopicAPI`.
* Reduced consumption of `shared cache` when approaching OOM.
* Merged the logic of schedulers from `data executer` and `scan executer` into one class.
* Added handles `discovery` and `proxy` to the execution process of `query` in `viewer backend`.
* Fixed an error where the handle `/cluster` returns the name of the root domain of type `/ru` in `viewer backend`.
* Implemented a scheme for seamless tablet updates for `QueryService`.
* Fixed an error where `DELETE` returned data and did not delete it.
* Fixed an error in the operation of `DELETE ON` in `query service`.
* Fixed the unexpected disabling of batching in the default schema settings.
* Fixed the triggered check `VERIFY failed: MoveUserTable(): requirement move.ReMapIndexesSize() == newTableInfo->Indexes.size()`.
* Increased the default timeout for grpc streaming.
* Excluded unused messages and methods from `QueryService`.
* Added sorting by `Rack` in `/nodes` in `viewer backend`.
* Fixed an error where a query with sorting returns an error when descending.
* Fixed the interaction of `QP` with `NodeWhiteboard`.
* Removed support for old parameter formats.
* Fixed an error where `DefineBox` was not applied to disks with a static group.
* Fixed an error `SIGSEGV` in dynodes when importing `CSV` via `YDB CLI`.
* Fixed an error with a crash when processing `NGRpcService::TRefreshTokenImpl`.
* Implemented the `gossip` protocol for exchanging information about cluster resources.
* Fixed an error:

  ```text
  DeserializeValuePickleV1(): requirement data.GetTransportVersion() ==
  (ui32) NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0 failed
  ```

* Implemented auto-increment columns.
* Use the status `UNAVAILABLE` instead of `GENERIC_ERROR` when identifying a shard error.
* Added support for `rope payload` in `TEvVGet`.
* Added ignoring of outdated events.
* Fixed the crash of write sessions on an invalid topic name.
* Fixed an error:

  ```text
  CheckExpected(): requirement newConstr failed, message: Rewrite error,
  missing Distinct((id)) constraint in node FlatMap
  ```

* Enabled `self heal` by default.
