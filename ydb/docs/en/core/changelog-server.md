# Change list {{ ydb-short-name }} Server

## Version 26.3 {#26-3}

### Release candidate 26.3.1.16 {#26-3-1-16-rc}

Release date: 18.09.26

#### Functionality

* [Export and import of backups for columnar tables, including S3-compatible storage, are available](./recipes/backup/backup-collections/exporting-to-external-storage.md?version=main).
* Columnar tables' columns support [dictionary encoding](./yql/reference/syntax/create_table/index.md?version=v26.3#encoding). Use `ENCODING(DICT)` for values with low cardinality.
* Columnar tables have [local min_max-indexes](./dev/min_max-skip-index.md?version=v26.3). They skip data fragments outside the query range, reducing the amount of data read. Use `ADD INDEX ... LOCAL USING min_max` to apply to a column.
* [Decommissioning of storage groups using virtual groups has been added](./maintenance/manual/virtual_storage_groups_decommit.md?version=v26.3). Data is moved to virtual groups in the background, applications continue to read and write.
* [Authentication via external OpenID Connect identity providers has been added](./security/authentication.md?version=v26.3#external-idp). {{ ydb-short-name }} verifies JWT tokens against the provider's JSON Web Key Set (JWKS) and periodically updates authentication data.
* Kafka API supports [mutual TLS authentication](./reference/kafka-api/auth.md?version=v26.3). The client certificate is matched with the security identifier, SASL authentication is not required.
* Columnar tables engine optimization: an updated compaction strategy is used for columnar tables, which organizes data more efficiently, and a new data merging strategy when reading, which speeds up queries on constantly changing data.
* Authentication/authorization subsystem optimization: batch authorization requests to AccessService are enabled by default, which reduces overhead.
* System virtual attributes such as `__ydb_create_time`, `__ydb_write_time`, etc., as well as user attributes `__ydb_user_attributes`, are now available for streaming YQL queries. [Link to functionality](./concepts/query_execution/topics.md?version=v26.3#system-metadata).
* Distributed Storage subsystem optimization: full VDisk synchronization has become faster by removing processed SyncLog data.
* Metrics and statistics for monitoring and diagnosing transfers have been added to `DescribeTransfer`.
* A configurable limit on the number of stored forced compaction operations has been added. Completed and cancelled operations can be automatically deleted after the limit is reached.
* Change Data Capture records may contain [OpenTelemetry trace identifier](./concepts/cdc.md?version=v26.3#record-structure) of the request that created the change.
* When [reading a topic from a timestamp](./reference/ydb-cli/topic-read.md?version=v26.3), messages with an earlier write time are filtered out, including from the same blob with newer messages.

#### Disabled functionality

The following functionality is not enabled by default.

* For columnar tables, you can run forced compaction using `ALTER TABLE ... COMPACT`.
* Parity between columnar and row tables has been achieved in terms of the set of YQL types (Interval, Uuid, DyNumber are supported).
* [Hybrid search](./dev/hybrid-search.md?version=v26.3) has been added, combining full-text relevance and vector proximity into a ranked result.
* Topics can be worked with via [Amazon SQS API](./reference/sqs-api/index.md?version=v26.3), using SQS-compatible clients to read and write messages.
* [JSON indexes](./dev/json-indexes.md?version=v26.3) have been added to speed up queries with `JSON_EXISTS` and `JSON_VALUE`.
* Full-text indexes support [filtering columns](./dev/fulltext-indexes.md?version=v26.3#filtered), allowing you to search in a logical section of the table.
* Full-text indexes can now be created for tables with [arbitrary primary key types](./dev/fulltext-indexes.md?version=v26.3#primary-key).

## Version 26.2 {#26-2}

### Version 26.2.1.14 {#26-2-1-14}

Release date: September 16, 2026.

#### Functionality

* [Full-text indexes](./dev/fulltext-indexes.md?version=v26.2) are enabled by default.
* [Streaming queries](./dev/streaming-query/index.md?version=v26.2) support reading from local topics, writing to local topics, reading local tables, and multiple `INSERT` instructions in one query.
* [Watermarks](./dev/streaming-query/watermarks.md?version=v26.2) are available in streaming queries.
* [Bloom indexes](./dev/bloom-skip-indexes.md?version=v26.2) have been added: Bloom and Bloom n-gram for columnar tables and prefix Bloom indexes for string tables.
* [Column compression](./yql/reference/syntax/create_table/index.md?version=v26.2) in columnar tables is available by default.
* You can now configure the [level of parallelism](./yql/reference/syntax/alter_table/indexes.md?version=v26.2) for building indexes.
* For string tables, [`ALTER TABLE`](./yql/reference/syntax/alter_table/columns.md?version=v26.2) instructions `ALTER COLUMN SET DEFAULT` and `ALTER COLUMN DROP DEFAULT` are available by default.
* For string tables, the YQL instruction [`TRUNCATE TABLE`](./yql/reference/syntax/truncate-table.md?version=v26.2) is available by default.
* The YQL instruction [`DISCARD SELECT`](./yql/reference/syntax/discard.md?version=v26.2) is available by default.
* QueryService supports returning query results in [Apache Arrow format](./reference/ydb-sdk/data-formats/format-arrow.md?version=v26.2); this feature is enabled by default.
* For string tables, it is possible to run forced [compaction](./yql/reference/syntax/alter_table/compact.md?version=v26.2) using `ALTER TABLE ... COMPACT`.
* Automatic storage balancing between groups and background checking of disk placement correctness have been added.
* Operations for splitting and merging tables with a large number of partitions have been accelerated: SchemeShard updates only the affected partitions instead of recalculating the entire list.
* [Audit logging](./security/audit-log.md?version=v26.2) of topic operations has been added.
* [Built-in collection of minidumps based on Google Breakpad](./devops/observability/minidumps.md?version=v26.2) for Linux nodes has been added.
* The [`ydb-dstool pdisk populate`](./reference/ydb-dstool/pdisk-populate.md?version=v26.2) subcommand has been added to replay PDisk load on another device.

#### Disabled functionality

The functionality is present in the core for the possibility of rolling back from the future 26-3 release, but is not enabled by default. It will be enabled by default in the next major release. It may also be enabled on some managed YDB services.

* Support for [incremental backups](./concepts/datamodel/backup-collection.md?version=v26.2) has been added, which allows saving only changes relative to the previous backup in the collection.
* [Export and import of columnar tables](./recipes/backup/import-export-column-tables.md?version=v26.2) via S3-compatible storage is supported.
* [Export and import of string tables](./reference/ydb-cli/export-import/export-nfs.md?version=main) via the local file system, including file systems mounted via NFS, have been added.
* Added snapshot retention for long analytical queries to columnar tables so that snapshot data is not deleted before the query completes.
* QueryService can notify the SDK about the completion of a node or session, so the client stops sending new requests there.
* Limits on the number and volume of small blobs at the database level have been added for columnar tables. If the hard limit is exceeded, new records are rejected.
* Expressions for [watermarks](./dev/streaming-query/watermarks.md?version=v26.2) can be calculated outside the context of a single message.
* [Min-max indexes](./yql/reference/syntax/create_table/min_max_index.md?version=main) have been added for columnar tables.
* [Dictionary encoding](./yql/reference/syntax/create_table/index.md?version=v26.2#encoding) of columns in columnar tables has been added.
* Online construction of unique secondary indexes has been added.
* An optimized conflict check has been added for transactions between topics and tables.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/46747) incorrect results of some scanning queries to columnar tables.
* [Fixed](https://github.com/ydb-platform/ydb/pull/50358) processing of corrupted Kafka requests, which could lead to excessive memory consumption or buffer overflow.
* [Fixed](https://github.com/ydb-platform/ydb/pull/49929) reading hang from a topic after restarting the read balancer.
* [Fixed](https://github.com/ydb-platform/ydb/pull/35470) race conditions in the server-side topic reading session and in [Topic SDK](https://github.com/ydb-platform/ydb/pull/42213).
* [Fixed](https://github.com/ydb-platform/ydb/pull/50897) crashes and [hangs](https://github.com/ydb-platform/ydb/pull/50621) of streaming queries when creating checkpoints.
* [Fixed](https://github.com/ydb-platform/ydb/pull/50379) race conditions when canceling and scheduling distributed transactions.
* [Fixed](https://github.com/ydb-platform/ydb/pull/49469) processing of too large blocks during encrypted export and [false data corruption error](https://github.com/ydb-platform/ydb/pull/48986) during encrypted recovery.
* [Fixed](https://github.com/ydb-platform/ydb/pull/49460) race condition when collecting statistics with the `ydb workload topic` command.
* [Fixed](https://github.com/ydb-platform/ydb/pull/48174) double memory release when completing `DqHashCombine` with spilling.
* [Fixed](https://github.com/ydb-platform/ydb/pull/40912) loss of `ReadSet` confirmations, which could prevent the completion of transactions with topics.
* [Fixed](https://github.com/ydb-platform/ydb/pull/40801) processing of the `NODATA` response in the KeyValue API: instead of crashing the process, an error `NOT_FOUND` or `INTERNAL_ERROR` is returned.
* [Fixed](https://github.com/ydb-platform/ydb/pull/41895) IAM authentication for external data sources in Generic Provider and [processing of errors returned by it](https://github.com/ydb-platform/ydb/pull/40761).
* [Fixed](https://github.com/ydb-platform/ydb/pull/41411) memory leak when loading metadata of external data sources.
* [Fixed](https://github.com/ydb-platform/ydb/pull/46739) processing of three-part feature flags in YAML configuration, which could lead to loss of following settings.
* [Fixed](https://github.com/ydb-platform/ydb/pull/41009) copying and exporting tables with secondary indexes after deleting internal index tables.
* [Fixed](https://github.com/ydb-platform/ydb/pull/45958) filtering of exported objects and operations with lists when exporting to the file system.
* [Fixed](https://github.com/ydb-platform/ydb/pull/47591) Metadata responses in the Kafka API that could contain an empty list of brokers or an inconsistent controller ID and lead to timeouts of Kafka AdminClient and Kafka Streams.
* [Fixed](https://github.com/ydb-platform/ydb/pull/46033) leak of records about script execution created by streaming queries.
* [Fixed](https://github.com/ydb-platform/ydb/pull/42277) overwriting of the user `config.yaml` mounted at the default path during the first deployment of `local-ydb` in Docker.
* [Fixed](https://github.com/ydb-platform/ydb/pull/44011) Hive crash after restart when the tablet lock and saved leader pointed to different nodes.

## Version 26.1 {#26-1}

### Version 26.1.1.22 {#26-1-1-22}

Release date: July 27, 2026.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/46894) authentication via Kafka API for local users: with the `DomainLoginOnly` setting enabled, users could not work with tenant databases.
* [Fixed](https://github.com/ydb-platform/ydb/pull/46946) crash (use-after-free) when updating the vector index due to asynchronous destruction of ReadActor.

### Version 26.1.1.20 {#26-1-1-20}

Release date: July 2, 2026.

#### Functionality

* YQL queries [`SHOW CREATE TABLE`](./yql/reference/syntax/show_create.md?version=v26.1) and [`SHOW CREATE VIEW`](./yql/reference/syntax/show_create.md?version=v26.1) are available to obtain DDL expressions needed to recreate the structure of a table or view.
* Support for default values has been added in `ALTER TABLE` when [`ADD COLUMN`](./yql/reference/syntax/alter_table/columns.md?version=v26.1) (`DEFAULT`).
* [Shuffle Elimination](./concepts/query_execution/optimizer.md?version=v26.1) has been enabled in production: the optimizer can eliminate unnecessary data redistributions during joins.
* [Backup and restore](./reference/ydb-cli/export-import/file-structure.md?version=v26.1) of schema objects has been implemented: [asynchronous replications](./concepts/async-replication.md?version=v26.1), [external data sources](./concepts/datamodel/external_data_source.md?version=v26.1), [external tables](./concepts/datamodel/external_table.md?version=v26.1), and [transfers](./concepts/transfer.md?version=v26.1).
* The cluster remains operational when [CMS](./concepts/glossary.md?version=v26.1#cms) is unavailable.
* The ability to [register dynamic nodes](./devops/configuration-management/configuration-v1/node-authorization.md?version=v26.1) using client TLS certificates has been added.
* For [LDAP authentication](./security/authentication.md?version=v26.1) of a service account, the SASL protocol with the EXTERNAL mechanism is supported — see [`enable_sasl_external_bind`](./reference/configuration/auth_config.md?version=v26.1#ldap-auth-config).
* [Asynchronous replication](./concepts/async-replication.md?version=v26.1) now supports mirroring [auto-partitioned topics](./concepts/datamodel/topic.md?version=v26.1#autopartitioning); see also [topic partitions in CDC](./concepts/cdc.md?version=v26.1#topic-partitions).
* The diagnosis of TLI (Transaction Lock Invalidation) has been expanded: [diagnosis](./reference/configuration/tli_config.md?version=v26.1) — configuration `tli_config`, [logging](./troubleshooting/performance/queries/tli-logging.md?version=v26.1) and [system views](./dev/system-views.md?version=v26.1#top-tli-partitions).
* When [auto-partitioning by load](./concepts/datamodel/table.md?version=v26.1#auto_partitioning_by_load) is used, CPU load on the partition leader and all its replicas is taken into account.
* [Stream queries](./dev/streaming-query/index.md?version=v26.1) now support [writing results to local tables](./dev/streaming-query/table-writing.md?version=v26.1).
* In [change data capture (CDC) streams](./concepts/cdc.md?version=v26.1), it is possible to export user security identifiers (`USER_SIDS`) — see [`ALTER TABLE` `CHANGEFEED`](./yql/reference/syntax/alter_table/changefeed.md?version=v26.1).
* For [external data sources](./concepts/datamodel/external_data_source.md?version=v26.1), `AUTH_METHOD=IAM` has been added.
* The CLI now supports authentication via a token file (`--token-file`).
* The operation of transactions between topics and tables has been optimized.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/34906) `RETURNING` in stream `UPDATE` and interactive queries.
* [Fixed](https://github.com/ydb-platform/ydb/pull/34915) `SET DEFAULT` and `DROP DEFAULT` in `ALTER TABLE`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/34958) the handling of authentication token expiration in the ticket parser.
* [Fixed](https://github.com/ydb-platform/ydb/pull/35003) query execution in the Workload Manager after tenant recreation.
* [Fixed](https://github.com/ydb-platform/ydb/pull/35187) use-after-free in the gRPC service.
* [Fixed](https://github.com/ydb-platform/ydb/pull/35663) incremental recovery during restarts of SchemeShard and shard failures.
* [Fixed](https://github.com/ydb-platform/ydb/pull/35787) the absence of stream query metadata immediately after creation.
* [Fixed](https://github.com/ydb-platform/ydb/pull/35793) async checkpointing hanging with full entry and empty checkpoint.
* [Fixed](https://github.com/ydb-platform/ydb/pull/36217) the mutual influence of quotas from different databases in the Kesus quoter proxy.
* [Fixed](https://github.com/ydb-platform/ydb/pull/36220) hangs in the PQ read session.
* [Fixed](https://github.com/ydb-platform/ydb/pull/36292) scan executor hanging on `SELECT … LIMIT` for empty tables.
* [Fixed](https://github.com/ydb-platform/ydb/pull/36692) delayed TLI reset `LOCKS_BROKEN`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/37130) stream query timeout overflow.
* [Fixed](https://github.com/ydb-platform/ydb/pull/37145) thread safety and token expiration parsing in the IAM credentials provider.
* [Fixed](https://github.com/ydb-platform/ydb/pull/37285) errors in the HTTP gateway.
* [Fixed](https://github.com/ydb-platform/ydb/pull/37668) overflow when processing an empty password.
* [Fixed](https://github.com/ydb-platform/ydb/pull/38033) CDC collection for columns `IsBuildInProgress` disabled.
* [Fixed](https://github.com/ydb-platform/ydb/pull/38490) segfault during update.
* [Fixed](https://github.com/ydb-platform/ydb/pull/38544) Shuffle Elimination with pragma `HashJoinMode`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/39337) duplication of rows in scan query due to delivery issues.
* [Fixed](https://github.com/ydb-platform/ydb/pull/39687) access to HTTP endpoints viewer: only viewer/admin SID; database scope for database-only tokens.
* [Fixed](https://github.com/ydb-platform/ydb/pull/39798) OOM when loading trash on blob depot.
* [Fixed](https://github.com/ydb-platform/ydb/pull/41681) crash of `TQueryBase` after canceling a stream query.
* [Fixed](https://github.com/ydb-platform/ydb/pull/43068) local CDC reading from YQL.
* [Fixed](https://github.com/ydb-platform/ydb/pull/44340) fast remote cancellation of queries in the query service.

## Version 25.4 {#25-4}

### Version 25.4.1.15 {#25-4-1-15}

Release date: June 5, 2026.

#### Functionality

* YQL instructions are available: [`BATCH UPDATE`](./yql/reference/syntax/batch-update.md?version=v25.4) and [`BATCH DELETE FROM`](./yql/reference/syntax/batch-delete.md?version=v25.4) for mass update and deletion of data in tables.
* The mechanism for executing write operations has changed significantly — now writes are performed in stream mode without fully materializing data on the Query Processor side before sending to data shards, which improves performance for large write operations. The change applies to some scenarios; in certain cases (for example, tables with secondary indexes), the previous approach is still used. For general information about the execution pipeline, see the section [Query execution](./concepts/query_execution/index.md?version=v25.4).
* Lookup Join execution has been optimized: it uses stream mode without materializing one side of the join, which reduces peak memory consumption, speeds up execution on large datasets, and removes previous limitations on the size of the join sides. See the description of [Index lookup Join](./faq/yql.md?version=v25.4#index-lookup-join) and the syntax of the [operator `JOIN`](./yql/reference/syntax/select/join.md?version=v25.4).
* It is now possible to set access rights for [system views](./devops/observability/system-views.md?version=v25.4) of the cluster and databases.
* For string tables, it is possible to configure [caching modes](./concepts/datamodel/table.md?version=v25.4#cache-modes) and a new mode `in_memory`, which allows preloading table data into RAM if sufficient RAM is available.
* For topic readers, a parameter has been added [`availability-period`](./reference/ydb-cli/topic-consumer-add.md?version=v25.4), which allows extending the storage of unacknowledged messages beyond the retention period.
* Partition-level topic metrics and export to user shard quotas [are available](./reference/observability/metrics/index.md?version=v25.4#topics_partitions) for monitoring and observability.
* Queries with `LIMIT` in columnar tables are faster due to early limiting of the sample on storage nodes (for queries without sorting or with sorting by the primary key). The general syntax of [`LIMIT` and `OFFSET`](./yql/reference/syntax/select/limit_offset.md?version=v25.4) in YQL.
* Columnar tables support the `Bool` type in the schema and queries — see [primitive YQL types](./yql/reference/types/primitive.md?version=v25.4#numeric).
* The [filterable vector index](./dev/vector-indexes.md?version=v25.4#filtered) now correctly finds rows with new filter column values inserted into the table after the index was created.
* Stream processing and data delivery are more tightly integrated into the core: [topic-to-table transfer](./concepts/transfer.md?version=v25.4), [streaming queries](./dev/streaming-query/index.md?version=v25.4) are available to users when 'EnableStreamingQueries' is enabled.
* An option `overlap_clusters` has been added to significantly improve the quality of vector search by placing vectors into several index clusters (index settings) — see [vector indexes](./dev/vector-indexes.md?version=v25.4).
* Search across all types of vector indexes has been significantly sped up by calculating distances locally on each datashard before network transmission — see [VIEW (vector index)](./yql/reference/syntax/select/vector_index.md?version=v25.4) and [vector indexes](./dev/vector-indexes.md?version=v25.4).
* Full vector search without an ANN index has been sped up using pushdown (vector search, KNN UDF) — see [vector search](./concepts/query_execution/vector_search.md?version=v25.4) and [KNN module](./yql/reference/udf/list/knn.md?version=v25.4).
* The mechanism for working with secrets stored in the database is now fully supported: creation, modification, deletion, and usage — see [Secrets](./concepts/datamodel/secrets.md?version=v25.4). Note that the [old syntax](./concepts/datamodel/secrets.md?version=v25.3) has been deprecated.
* The execution of [`UNION ALL`](./yql/reference/syntax/select/union.md?version=v26.2#union-all) has been improved: parallel execution is now supported, which increases the performance of analytical queries.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/38425) vulnerability in [LDAP authentication](./security/authentication.md): knowing the login and password of any LDAP user (including those not in the group with access to {{ ydb-short-name }}), it was possible to bypass the group membership check and gain access to the cluster (injection into the LDAP user search filter; special characters are now escaped according to RFC 2254).

## Version 25.3 {#25-3}

### Version 25.3.1.27 {#25-3-1-27}

Release date: May 20, 2026.

#### Functionality

* Support for configuring two DCs with synchronous data writing (mode [`Bridge`](./concepts/bridge.md)) has been added; available in {{ ydb-short-name }} Enterprise.
* Topic improvements:
  * In the Kafka API, it is now possible to create [compacted](https://docs.confluent.io/kafka/design/log_compaction.html#ak-log-compaction) topics; YDB automatically creates and deletes an internal service consumer used for topic compaction;
  * The topic API has been expanded: new parameters have been added to the `DescribeConsumer` output [new parameters](./reference/ydb-sdk/topic.md), and [partition-level topic metrics can be delivered to user quotas](./reference/observability/metrics/index.md#topics).
* [Backup and restore](./reference/ydb-cli/export-import/file-structure.md?version=v25.3#topics) of the topic configuration to and from S3 has been implemented.
* [Export of views](./reference/ydb-cli/export-import/file-structure.md#views) (`VIEW`) to and from S3 has been implemented.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/38425) vulnerability in [LDAP authentication](./security/authentication.md): knowing the login and password of any LDAP user (including those not in the group with access to {{ ydb-short-name }}), it was possible to bypass the group membership check and gain access to the cluster (injection into the LDAP user search filter; special characters are now escaped according to RFC 2254).
* [Fixed](https://github.com/ydb-platform/ydb/pull/33758) a bug that led to session leakage on the server side.
* [Fixed](https://github.com/ydb-platform/ydb/pull/36926) a bug that in rare cases could cause table reads to block its deletion.
* [Fixed](https://github.com/ydb-platform/ydb/pull/20238) a race condition when updating the soft CPU limit.
* [Fixed behavior](https://github.com/ydb-platform/ydb/pull/18121) that caused `ALTER TABLE` to fail for tables with a vector index.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18088) inconsistent results in some read-write transactions — conflicting writes no longer overwrite uncommitted changes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18234) serializability violations in read-write transactions after shard restarts.
* [Fixed](https://github.com/ydb-platform/ydb/pull/20560) a memory management error when committing an offset in topics with automatic partitioning enabled.
* [Added](https://github.com/ydb-platform/ydb/pull/18698) checks for enabled encryption in zero-copy transfer.
* [Fixed](https://github.com/ydb-platform/ydb/pull/20519) an error causing VDisk to hang in local recovery after an error `ChunkRead`.
* [Eliminated](https://github.com/ydb-platform/ydb/pull/18924) the appearance of phantom VDisks due to races between group creation and deletion operations.
* [Improved](https://github.com/ydb-platform/ydb/pull/17687) the determination of the PDisk state — now it uses the actual state from BSC, which improves the accuracy of healthcheck.
* When ending a session through attach stream, a [notification is now sent](https://github.com/ydb-platform/ydb/pull/22298).
* The coordination service now correctly [returns](https://github.com/ydb-platform/ydb/pull/16901) code `SCHEME_ERROR` for non-existent resources instead of the erroneously used code `INTERNAL_ERROR`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/20157) memory errors and data inconsistencies in the Workload Manager and related scheduler code.
* [Fixed](https://github.com/ydb-platform/ydb/pull/20432) an issue that caused PDisk information queries to time out if the target node was offline or unavailable.

## Version 25.2 {#25-2}

### Version 25.2.1.26 {#25-2-1-26}

Release date: May 12, 2026.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/38425) vulnerability in [LDAP authentication](./security/authentication.md): knowing the login and password of any LDAP user (including those not in the group with access to {{ ydb-short-name }}), it was possible to bypass the group membership check and gain access to the cluster (injection into the LDAP user search filter; special characters are now escaped according to RFC 2254).
* [Fixed](https://github.com/ydb-platform/ydb/pull/25112) [an issue](https://github.com/ydb-platform/ydb/issues/23858) that could cause [tablet](./concepts/glossary.md#tablet) deletion to hang
* [Fixed](https://github.com/ydb-platform/ydb/pull/25145) [an error](https://github.com/ydb-platform/ydb/issues/20866) causing an error when changing the table's follower
* A number of bugs related to [changefeed](./concepts/glossary.md#changefeed) have been fixed:
  * [Fixed](https://github.com/ydb-platform/ydb/pull/25689) [bug](https://github.com/ydb-platform/ydb/issues/25524) that could cause the import of a table with a Utf8 key and enabled changefeed to fail
  * [Fixed](https://github.com/ydb-platform/ydb/pull/25453) [bug](https://github.com/ydb-platform/ydb/issues/25454) when importing a table without change streams could fail due to incorrect changefeed file search
* [Fixed](https://github.com/ydb-platform/ydb/pull/26069) [bug](https://github.com/ydb-platform/ydb/issues/25869) that could lead to failures during UPSERT operations in columnar tables
* [Fixed](https://github.com/ydb-platform/ydb/pull/26504) [bug](https://github.com/ydb-platform/ydb/issues/26225) that caused a crash due to accessing already freed memory
* [Fixed](https://github.com/ydb-platform/ydb/pull/26657) [bug](https://github.com/ydb-platform/ydb/issues/23122) with duplicates in unique secondary indexes
* [Fixed](https://github.com/ydb-platform/ydb/pull/26879) [bug](https://github.com/ydb-platform/ydb/issues/26565) of incorrect checksum matching when restoring compressed backups from S3
* [Fixed](https://github.com/ydb-platform/ydb/pull/27528) [bug](https://github.com/ydb-platform/ydb/issues/27193) that could cause some TPC-H 1000 benchmark queries to fail
* A number of issues related to cluster initialization have been fixed:
  * [Fixed](https://github.com/ydb-platform/ydb/pull/25678) [bug](https://github.com/ydb-platform/ydb/issues/25023) that could cause cluster initialization to hang with mandatory authorization
  * [Fixed](https://github.com/ydb-platform/ydb/pull/28886) [issue](https://github.com/ydb-platform/ydb/issues/27228) that made it impossible to create new databases immediately after deploying the cluster for several minutes
* [Fixed](https://github.com/ydb-platform/ydb/pull/28655) [bug](https://github.com/ydb-platform/ydb/issues/28510) that could cause a race condition and clients to receive error `Could not find correct token validator` if recently issued tokens were used before the `LoginProvider` state was updated
* [Fixed](https://github.com/ydb-platform/ydb/pull/29940) [bug](https://github.com/ydb-platform/ydb/issues/29903) where a named expression containing another named expression led to incorrect backup of `VIEW`

### Release candidate 25.2.1.10 {#25-2-1-10-rc}

Release date: September 21, 2025.

#### Functionality

* [Analytical capabilities](./concepts/analytics/index.md) are enabled by default: [columnar tables](./concepts/datamodel/table.md#column-oriented-tables) can be created without enabling special flags, using LZ4 compression and hash partitioning. Supported operations include a wide range of DML (UPDATE, DELETE, UPSERT, INSERT INTO ... SELECT) and CREATE TABLE AS SELECT. Integration with dbt, Apache Airflow, Jupyter, Superset and federated queries to S3 allow building end-to-end analytical pipelines in YDB.
* [Cost optimizer](./concepts/query_execution/optimizer.md) is enabled by default for queries using at least one columnar table, but can be forced for other queries as well. The cost optimizer improves query execution performance by calculating the optimal order and type of joins based on table statistics; supported [hints](./dev/optimization/hints.md) allow fine-tuning execution plans for complex analytical queries.
* [Data transfer](./concepts/transfer.md) has been implemented — an asynchronous mechanism for transferring data from a topic to a table. [Creating](./yql/reference/syntax/create-transfer.md) a transfer instance, its [modification](./yql/reference/syntax/alter-transfer.md) and [deletion](./yql/reference/syntax/drop-transfer.md) are done using YQL. For a quick start, use the [example instruction](./recipes/transfer/quickstart.md).
* [Spilling](./concepts/query_execution/spilling.md), a memory management mechanism, has been added, where intermediate data generated during query execution and exceeding the available RAM of the node is temporarily offloaded to external storage. Spilling enables execution of user queries that require processing large amounts of data exceeding the node's available memory.
* The [maximum time for executing a single query](./concepts/limits-ydb?version=v25.2) has been increased from 30 minutes to 2 hours.
* Support for Certificate Authority (CA) and [Yandex Cloud Identity and Access Management (IAM)](https://yandex.cloud/ru/docs/iam) authentication in [asynchronous replication](./yql/reference/syntax/create-async-replication.md?version=v25.2) has been added.
* Mandatory to configure:
  * [Authentication and authorization of nodes](./devops/configuration-management/configuration-v1/node-authorization.md) for registering nodes in the cluster.
* Enabled by default:
  * [Vector index](./dev/vector-indexes.md) for approximate vector search;
  * support for [YDB Topics Kafka API](./reference/kafka-api/index.md) [client reader balancing](https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb), [compacted topics](https://docs.confluent.io/kafka/design/log_compaction.html) and [transactions](https://www.confluent.io/blog/transactions-apache-kafka);
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

## Version 25.1 {#25-1}

### Version 25.1.4.18 {#25-1-4-18}

Release date: May 12, 2026.

#### Functionality

* [Added](https://github.com/ydb-platform/ydb/pull/21119) the ability to use familiar tools for stream data processing — Kafka Connect, Confluent Schema Registry, Kafka Streams, Apache Flink, AKH via [Kafka API](./reference/kafka-api/index.md) when working with YDB Topics. Now YDB Topics Kafka API supports:
  * client load balancing for readers — enabled by setting the `enable_kafka_native_balancing` flag in the [cluster configuration](./reference/configuration/feature_flags.md). [How reader balancing works in Apache Kafka](https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb). Now reader balancing in YDB Topics Kafka API will work the same way,
  * [compacted topics](https://docs.confluent.io/kafka/design/log_compaction.html) — enabled by setting the `enable_topic_compactification_by_key` flag,
  * [transactions](https://www.confluent.io/blog/transactions-apache-kafka) — enabled by setting the `enable_kafka_transactions` flag.
* [Added](https://github.com/ydb-platform/ydb/pull/20982) a [new protocol](https://github.com/ydb-platform/ydb/issues/11064) in [Node Broker](./concepts/glossary.md#node-broker), eliminating spikes in network traffic on large clusters (more than 1000 servers) associated with broadcasting node information.

#### YDB UI

* [Fixed](https://github.com/ydb-platform/ydb/pull/17839) a [bug](https://github.com/ydb-platform/ydb/issues/15230) that caused not all tablets to be displayed on the Tablets tab in the diagnostics section.
* Fixed a [bug](https://github.com/ydb-platform/ydb/issues/18735) that caused the Storage tab in the database diagnostics section to display not only storage nodes.
* Fixed a [serialization error](https://github.com/ydb-platform/ydb-embedded-ui/issues/2164) that could lead to a crash when opening query execution statistics.
* Changed the logic for nodes transitioning to a critical state — a CPU pool filled to 75-99% now triggers a warning rather than a critical state.

#### Performance

* [Optimized](https://github.com/ydb-platform/ydb/pull/20197) the processing of empty inputs when executing JOIN operations.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/38425) a vulnerability in [LDAP authentication](./security/authentication.md): knowing the login and password of any LDAP user (including those not in the access group for {{ ydb-short-name }}), it was possible to bypass the group membership check and gain access to the cluster (injection into the LDAP search filter; special characters are now escaped according to RFC 2254).
* [Added](https://github.com/ydb-platform/ydb/pull/21918) support in asynchronous replication for a new type of change record — `reset` records (in addition to `update` and `erase` records).
* [Fixed](https://github.com/ydb-platform/ydb/pull/21836) a [bug](https://github.com/ydb-platform/ydb/issues/21814) that caused a replication instance with an unspecified `COMMIT_INTERVAL` parameter to crash the process.
* [Fixed](https://github.com/ydb-platform/ydb/pull/21652) rare errors when reading from a topic during partition balancing.
* [Fixed](https://github.com/ydb-platform/ydb/pull/22455) a bug that caused system tablets of a dedicated database to remain undeleted when the database was deleted.
* [Fixed](https://github.com/ydb-platform/ydb/pull/22203) a bug that caused tablets to hang when there was insufficient memory on the nodes. Now tablets will automatically start as soon as sufficient resources become available on any node.
* [Fixed](https://github.com/ydb-platform/ydb/pull/24278) a bug that caused only the first message from a batch to be saved when writing Kafka messages, while the rest of the messages were ignored.

### Release candidate 25.1.2.7 {#25-1-2-7-rc}

Release date: July 14, 2025.

#### Functionality

* [Implemented](https://github.com/ydb-platform/ydb/pull/19504) a [vector index](./dev/vector-indexes.md?version=v25.1) for approximate vector search. Recipes for [YDB CLI and YQL](./recipes/vector-search?version=v25.1) have been published for vector search, as well as examples of work [in C++ and Python](./recipes/ydb-sdk/vector-search?version=v25.1).
* [Added](https://github.com/ydb-platform/ydb/issues/11454) support for [consistent asynchronous replication](./concepts/async-replication.md?version=v25.1).
* Added a [V2 configuration mechanism](./devops/configuration-management/configuration-v2/config-overview?version=v25.1) that simplifies the deployment of new {{ ydb-short-name }} clusters and further work with them. [Comparison](./devops/configuration-management/compare-configs?version=v25.1) of V1 and V2 configuration mechanisms.
* Added support for parameterized [Decimal type](./yql/reference/types/primitive.md?version=v25.1#numeric).
* [Added](https://github.com/ydb-platform/ydb/pull/8065) the ability to not use the `DECLARE` operator to declare parameter types in queries. Now parameter types are determined automatically based on the passed values.
* Implemented client-side partition balancing when reading via the [Kafka protocol](https://kafka.apache.org/documentation/#consumerconfigs_partition.assignment.strategy) (like Kafka itself). Previously, balancing occurred on the server. Enabled by setting the `enable_kafka_native_balancing` flag in the cluster configuration.
* Added support for [auto-partitioning of topics](./concepts/cdc.md?version=v25.1#topic-partitions) in CDC for string tables. Enabled by setting the `enable_topic_autopartitioning_for_cdc` flag in the cluster configuration.
* [Added](https://github.com/ydb-platform/ydb/pull/8264) the ability to [change the data retention time](./concepts/cdc.md?version=v25.1#topic-options) in a CDC topic using the `ALTER TOPIC` expression.
* [Supported](https://github.com/ydb-platform/ydb/pull/7052) the [DEBEZIUM_JSON format](./concepts/cdc.md?version=v25.1#debezium-json-record-structure) for change streams (changefeed).
* [Added](https://github.com/ydb-platform/ydb/pull/19507) the ability to create change streams for index tables.
* Added the ability to [specify the number of replicas](./yql/reference/syntax/alter_table/indexes.md?version=v25.1) for a secondary index. Enabled by setting the `enable_access_to_index_impl_tables` flag in the cluster configuration.
* The range of supported objects has been expanded in backup and restore operations. Enabled by setting the flags specified in parentheses:
  * [support](https://github.com/ydb-platform/ydb/issues/7054) for change streams (flags `enable_changefeeds_export` and `enable_changefeeds_import`);
  * [support](https://github.com/ydb-platform/ydb/issues/12724) for views (`VIEW`) (flag `enable_view_export`).
* Added automatic deletion of temporary directories and tables when exporting to S3. Enabled by setting the `enable_export_auto_dropping` flag in the cluster configuration.
* [Added](https://github.com/ydb-platform/ydb/pull/12909) automatic integrity checking of backups during import, preventing restoration from corrupted backups and protecting against data loss.
* [Added](https://github.com/ydb-platform/ydb/pull/15570) the ability to create views using [UDF](./yql/reference/builtins/basic.md?version=v25.1#udf) in queries.
* Added system views with information about [access rights settings](./dev/system-views?version=v25.1#auth), [the history of overloaded partitions](./dev/system-views?version=v25.1#top-overload-partitions) — enabled by setting the `enable_followers_stats` flag in the cluster configuration, [the history of partitions of string tables with broken locks (TLI)](./dev/system-views?version=v25.1#top-tli-partitions).
* Added new parameters to the [CREATE USER](./yql/reference/syntax/create-user.md?version=v25.1) and [ALTER USER](./yql/reference/syntax/alter-user.md?version=v25.1) statements:
  * `HASH` — the ability to set a password in encrypted form;
  * `LOGIN` and `NOLOGIN` — unlocking and blocking a user.
* Enhanced account security:
  * [Added](https://github.com/ydb-platform/ydb/pull/11963) [password complexity check](./reference/configuration/?version=v25.1#password-complexity) for users;
  * [Implemented](https://github.com/ydb-platform/ydb/pull/12578) [automatic user blocking](./reference/configuration/?version=v25.1#account-lockout) when the password attempt limit is exhausted;
  * [Added](https://github.com/ydb-platform/ydb/pull/12983) the ability for users to change their password independently.
* [Implemented](https://github.com/ydb-platform/ydb/issues/9748) the ability to switch functional flags while the {{ ydb-short-name }} server is running. Flags that do not have the `(RequireRestart) = true` parameter specified in the [proto file](https://github.com/ydb-platform/ydb/blob/main/ydb/core/protos/feature_flags.proto#L60) will be applied without restarting the cluster.
* Now the oldest (rather than new) locks [are changed to full-shard](https://github.com/ydb-platform/ydb/pull/11329) when the number of locks on shards is exceeded.
* [Implemented](https://github.com/ydb-platform/ydb/pull/12567) the preservation of optimistic locks in memory during a smooth restart of data shards, which should reduce the number of ABORTED errors due to loss of locks when balancing tables between nodes.
* [Implemented](https://github.com/ydb-platform/ydb/pull/12689) the cancellation of volatile transactions with ABORTED status during a smooth restart of data shards.
* [Added](https://github.com/ydb-platform/ydb/pull/6342) the ability to remove `NOT NULL` constraints on a column in a table using the `ALTER TABLE ... ALTER COLUMN ... DROP NOT NULL` query.
* [Added](https://github.com/ydb-platform/ydb/pull/9168) a limit of 100,000 on the number of simultaneous requests to create sessions in the coordination service.
* [Increased](https://github.com/ydb-platform/ydb/pull/14219) the maximum [number of columns in the primary key](./concepts/limits-ydb.md?version=v25.1#schema-object) from 20 to 30.
* Improved diagnosis and introspection of memory-related errors ([#10419](https://github.com/ydb-platform/ydb/pull/10419), [#11968](https://github.com/ydb-platform/ydb/pull/11968)).
* **_(Experimentally)_** [Added](https://github.com/ydb-platform/ydb/pull/14075) an experimental mode of operation with more stringent access rights checks. Enabled by setting the following flags:
  * `enable_strict_acl_check` — do not allow granting rights to non-existent users and deleting users if they have been granted rights;
  * `enable_strict_user_management` — enables strict rules for administering local users (i.e., only the cluster or database administrator can administer local users);
  * `enable_database_admin` — adds the database administrator role.

#### Backward compatibility changes

* If you use queries that refer to named expressions as tables using [AS_TABLE](./yql/reference/syntax/select/from_as_table?version=v25.1), update [temporal over YDB](https://github.com/yandex/temporal-over-ydb) to version [v1.23.0-ydb-compat](https://github.com/yandex/temporal-over-ydb/releases/tag/v1.23.0-ydb-compat) before updating YDB to the current version to avoid errors in executing such queries.

#### YDB UI

* The query editor [has been updated](https://github.com/ydb-platform/ydb-embedded-ui/pull/1974) with support for partial loading of results — display starts immediately upon receiving the first fragment from the server without waiting for the query to complete fully. This allows for faster retrieval of results.
* [Improved](https://github.com/ydb-platform/ydb-embedded-ui/pull/1967) security: controls that are unavailable to the user are no longer displayed in the interface. Users will not encounter "Access denied" errors.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1981) search by tablet ID to the "Tablets" tab.
* Added a shortcut key hint that opens with the `⌘+K` combination.
* A "Operations" tab has been added to the database page, which allows you to view a list of operations and cancel them.
* The cluster monitoring panel has been updated, and the option to collapse it has been added.
* Support for case-sensitive search has been added in the hierarchical JSON display tool.
* Examples of code for connecting to YDB SDK have been added to the top panel after selecting a database, which speeds up the development process.
* The sorting of rows in the "Queries" tab has been fixed.
* Unnecessary confirmation requests when closing the browser page in the query editor have been removed — confirmation is only requested when necessary.

#### Performance

* [Added](https://github.com/ydb-platform/ydb/pull/6509) support for [constant folding](https://en.wikipedia.org/wiki/Constant_folding) in the query optimizer by default, which improves query performance by calculating constant expressions at the compilation stage.
* [Added](https://github.com/ydb-platform/ydb/issues/6512) a new granular timecast protocol, which will reduce the execution time of distributed transactions (slowing down one shard will not slow down all others).
* [Implemented](https://github.com/ydb-platform/ydb/issues/11561) the functionality of saving the state of data shards in memory during restarts, which allows preserving locks and increasing the chances of successful transaction execution. This reduces the execution time of long transactions by reducing the number of retries.
* [Implemented](https://github.com/ydb-platform/ydb/pull/15255) pipelined processing of internal transactions in [Node Broker](./concepts/glossary?version=v25.1#node-broker), which has sped up the launch of dynamic nodes in the {{ ydb-short-name }} cluster.
* [Improved](https://github.com/ydb-platform/ydb/pull/15607) the stability of Node Broker under increased load from cluster nodes.
* [Enabled](https://github.com/ydb-platform/ydb/pull/19440) by default, unloadable B-Tree indexes instead of non-unloadable SST indexes, which reduces memory consumption when storing "cold" data.
* [Optimized](https://github.com/ydb-platform/ydb/pull/15264) memory consumption by storage nodes.
* [Reduced](https://github.com/ydb-platform/ydb/pull/10969) Hive startup time by 30%.
* [Optimized](https://github.com/ydb-platform/ydb/pull/6561) the replication process in the distributed storage.
* [Optimized](https://github.com/ydb-platform/ydb/pull/9491) the size of the header for large binary objects in VDisk.
* [Reduced](https://github.com/ydb-platform/ydb/pull/15517) memory consumption by cleaning allocator pages.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/9707) an error in Interconnect configuration [Interconnect](./concepts/glossary.md?version=v25.1#actor-system-interconnect) that caused performance degradation.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13993) the «Out of memory» error when deleting very large tables by regulating the number of tablets processing the operation simultaneously.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9848) an error that occurred when specifying the same database node multiple times in the configuration for system tablets.
* [Fixed](https://github.com/ydb-platform/ydb/pull/11059) the error of long (seconds) data reading during frequent table resharding operations.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9723) an error in reading from asynchronous replicas that led to a failure.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9507) rare freezes during the initial scanning of [CDC](./dev/cdc.md?version=v25.1).
* [Fixed](https://github.com/ydb-platform/ydb/pull/11483) the handling of unfinished schema transactions in datashards during system restart.
* [Fixed](https://github.com/ydb-platform/ydb/pull/10460) the error of inconsistent reading from a topic when trying to explicitly acknowledge a message read within a transaction. Now the user will receive an error when trying to acknowledge the message.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12220) an error that caused autopartitioning to work incorrectly when working with a topic in a transaction.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12905) transaction freezes when working with topics during tablet restart.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13910) the «Key is out of range» error when importing from an S3-compatible storage.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13741) incorrect determination of the end of a metadata field in the cluster configuration.
* [Improved](https://github.com/ydb-platform/ydb/pull/16420) the construction of secondary indexes: when certain errors occur, the system retries the process rather than interrupting it.
* [Fixed](https://github.com/ydb-platform/ydb/pull/16635) an error in executing the expression `RETURNING` in `INSERT INTO` and `UPSERT INTO` queries.
* [Fixed](https://github.com/ydb-platform/ydb/pull/16269) the «Drop Tablet» operation freeze in PQ tablet, especially during Interconnect delays.
* [Fixed](https://github.com/ydb-platform/ydb/pull/16194) an error that occurred during [compaction](./concepts/glossary.md?version=v25.1#compaction) of VDisk.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15233) an issue that caused long-running topic read sessions to end with «too big inflight» errors.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15515) a freeze when reading a topic if at least one partition had no incoming data but was being read by multiple consumers.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18614) a rare issue with PQ tablet reboots.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18378) an issue where after updating the cluster version, Hive subscribers were started in data centers without running database nodes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/19057) error `Failed to set up listener on port 9092 errno# 98 (Address already in use)` that occurred during version update.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18905) an error that led to a segmentation fault when executing a healthcheck request and disabling a cluster node simultaneously.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18899) a failure in [string table partitioning](./concepts/datamodel/table.md?version=v25.1#partitioning_row_table) when selecting a partitioned key from access samples containing mixed operations with the full key and key prefix (e.g., exact reading or range reading).
* [Fixed](https://github.com/ydb-platform/ydb/pull/16797) an error that caused topic autopartitioning to not work when the `max_active_partition` configuration parameter was set using the `ALTER TOPIC` expression.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18938) an error that caused `ydb scheme describe` to return a list of columns in a different order than they were specified when creating the table.

## Version 24.4 {#24-4}

### Version 24.4.4.12 {#24-4-4-12}

Release date: June 3, 2025.

#### Performance

* [Limited](https://github.com/ydb-platform/ydb/pull/17755) the number of configuration changes being processed simultaneously.
* [Optimized](https://github.com/ydb-platform/ydb/issues/18289) PQ tablet memory consumption.
* [Optimized](https://github.com/ydb-platform/ydb/issues/18473) Scheme shard tablet CPU consumption, which reduced response delays to requests. Now the limit on the number of Scheme shard operations is checked before performing partitioning and merging operations.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/17123) a rare freeze of client applications during transaction commit execution when a partition deletion occurred before updating the write quota for the topic.
* [Fixed](https://github.com/ydb-platform/ydb/pull/17312) an error in copying tables with the Decimal type, which caused a failure when rolling back to a previous version.
* [Fixed](https://github.com/ydb-platform/ydb/pull/17519) [an error](https://github.com/ydb-platform/ydb/issues/17499) where a commit without acknowledging a write to the topic led to a block of the current and subsequent transactions with topics.
* Fixed transaction freezes when working with topics during [reboot](https://github.com/ydb-platform/ydb/issues/17843) or [deletion](https://github.com/ydb-platform/ydb/issues/17915) of a tablet.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18114) [issues](https://github.com/ydb-platform/ydb/issues/18071) with reading messages larger than 6Mb via [Kafka API](./reference/kafka-api).
* [Fixed](https://github.com/ydb-platform/ydb/pull/18319) a memory leak during writing to [a topic](./concepts/glossary#topic).
* Fixed errors in handling [nullable columns](https://github.com/ydb-platform/ydb/issues/15701) and [UUID type columns](https://github.com/ydb-platform/ydb/issues/15697) in string tables.

### Version 24.4.4.2 {#24-4-4-2}

Release date: April 15, 2025.

#### Functionality

* Enabled by default:

  * support for [views (VIEW)](./concepts/datamodel/view.md){% if feature_view %}{% else %}views (VIEW){% endif %};
  * [auto-partitioning](./concepts/datamodel/topic.md#autopartitioning) mode for topics;
  * [transactions involving topics and string tables](./concepts/transactions.md#topic-table-transactions);
  * [volatile distributed transactions](./contributor/datashard-distributed-txs.md#osobennosti-vypolneniya-volatilnyh-tranzakcij).

* Added the ability to [read and write to a topic](./reference/kafka-api/examples.md#primery-raboty-s-kafka-api) using Kafka API without authentication.

#### Performance

* [Automatic selection of a secondary index](./dev/secondary-indexes.md#avtomaticheskoe-ispolzovanie-indeksov-pri-vyborke) when executing a query is enabled by default.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/14811) an error that led to a significant decrease in reading speed from [tablet subscribers](./concepts/glossary.md#tablet-follower).
* [Fixed](https://github.com/ydb-platform/ydb/pull/14516) an error that caused waiting for confirmation of a volatile distributed transaction until the next restart.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15077) a rare error that caused a failure when connecting tablet subscribers to the leader with an inconsistent command log state.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15074) a rare error that caused a failure when restarting a remote datashard with inconsistent changes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15194) an error that could violate the order of message processing in the topic.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15308) a rare error that could cause reading from the topic to hang.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15160) an issue where a transaction hung when a user was simultaneously managing a topic and moving a PQ tablet to another node.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15233) an issue with a counter value leak for userInfo, which could lead to a `too big in flight` reading error.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15467) a proxy server crash due to duplicate topics in the request.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15933) a rare error that allowed a user to write to a topic bypassing account quota restrictions.
* [Fixed](https://github.com/ydb-platform/ydb/pull/16288) an issue where, after deleting a topic, the system returned "OK", but the tablets continued to operate. To delete such tablets, use the instructions from [pull request](https://github.com/ydb-platform/ydb/pull/16288).
* [Fixed](https://github.com/ydb-platform/ydb/pull/16418) a rare error that prevented the restoration of a backup of a large table with a secondary index.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15862) an issue that caused an error when inserting data using `UPSERT` into string tables with default values.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15334) an error that caused a failure when executing queries to tables with secondary indexes that return result lists using the `RETURNING *` expression.

## Version 24.3 {#24-3}

### Version 24.3.15.5 {#24-3-15-5}

Release date: February 6, 2025.

#### Functionality

* Added the ability to register a [database node](./concepts/glossary.md#database-node) using a certificate. A flag for using a certificate during registration has been added to [Node Broker](./concepts/glossary.md#node-broker) `AuthorizeByCertificate`.
* [Added](https://github.com/ydb-platform/ydb/pull/11775) priorities for authenticating tickets [using a third-party IAM provider](./security/authentication.md#iam), with the highest priority given to requests from new users. Tickets in the cache update their information with a lower priority.

#### Performance

* [Accelerated](https://github.com/ydb-platform/ydb/pull/12747) the lifting of tablets on large clusters: 210 ms **→** 125 ms (SSD), 260 ms **→** 165 ms (HDD).

#### Bug fixes

* [Removed](https://github.com/ydb-platform/ydb/pull/11901) the restriction on writing values greater than 127 to the Uint8 type.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12221) an error that caused a significant increase in CPU load when reading small messages from a topic in small portions. This could lead to delays in reading/writing to this topic.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12915) an error in restoring from a backup saved in an S3 storage with Path-style addressing.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13918) an error in restoring from a backup that was created during automatic table splitting.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12601) an error in serialization `Uuid` for [CDC](./concepts/cdc.md).
* [Fixed](https://github.com/ydb-platform/ydb/pull/12018) a potential failure of ["frozen" locks](./contributor/datashard-locks-and-change-visibility#vzaimodejstvie-s-raspredelyonnymi-tranzakciyami), which could be caused by mass operations (for example, deletion by TTL).
* [Fixed](https://github.com/ydb-platform/ydb/pull/12804) an error where reading on tablet subscribers could cause failures during automatic table splitting.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12807) an error where the [coordination node](./concepts/datamodel/coordination-node.md) successfully registered proxy servers despite a connection break.
* [Fixed](https://github.com/ydb-platform/ydb/pull/11593) an error that occurred when opening a tab with information about [distributed storage groups](./concepts/glossary.md#storage-group) in the interface.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12448) [an error](https://github.com/ydb-platform/ydb/issues/12443) where [Health Check](./reference/ydb-sdk/health-check-api) did not report time synchronization issues.
* [Fixed](https://github.com/ydb-platform/ydb/pull/11658) a rare issue that led to errors when executing a read query.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13501) a rare issue that led to leaks of uncommitted changes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13948) consistency issues related to caching remote ranges.

### Version 24.3.11.14 {#24-3-11-14}

Release date: January 9, 2025.

* [Supported](https://github.com/ydb-platform/ydb/pull/11276) restart without loss of cluster availability in [minimum fault-tolerant configuration](./concepts/topology#reduced) of three nodes.
* [Added](https://github.com/ydb-platform/ydb/pull/13218) new UDF Roaring bitmap functions: AndNotWithBinary, FromUint32List, RunOptimize.

### Version 24.3.11.13 {#24-3-11-13}

Release date: December 24, 2024.

#### Functionality

* Added [query tracing](./reference/observability/tracing/setup) — a tool that allows you to view in detail the path of a request through a distributed system.
* Added support for [asynchronous replication](./concepts/async-replication), which allows you to synchronize data between YDB databases almost in real time. It can also be used to migrate data between databases with minimal downtime for applications working with them.
* Added support for [views (VIEW)](https://ydb.tech/docs/en/concepts/datamodel/view), which can be enabled by the cluster administrator using the `enable_views` setting in the [dynamic configuration](./devops/configuration-management/configuration-v1/dynamic-config#obnovlenie-dinamicheskoj-konfiguracii).
* In [federated queries](./concepts/query_execution/federated_query/), support for new external data sources has been added: MySQL, Microsoft SQL Server, Greenplum.
* Developed [documentation](./devops/deployment-options/manual/federated-queries/connector-deployment) on deploying YDB with federated query functionality (manually).
* For the YDB Docker container, a startup parameter has been added `FQ_CONNECTOR_ENDPOINT`, allowing you to specify the address of the connector to external data sources. Added the ability to TLS-encrypt the connection with the connector. Added the ability to output the port of the connector service running locally on the same host as the dynamic YDB node.
* Added [auto-partitioning](./concepts/datamodel/topic#autopartitioning) mode for topics, in which topics can split partitions depending on the load while maintaining guarantees of message read order and exactly once writing. The mode can be enabled by the cluster administrator using the `enable_topic_split_merge` and `enable_pqconfig_transactions_at_scheme_shard` settings in the [dynamic configuration](./devops/configuration-management/configuration-v1/dynamic-config#obnovlenie-dinamicheskoj-konfiguracii).
* Added [transactions](./concepts/transactions#topic-table-transactions) involving [topics](https://ydb.tech/docs/en/concepts/datamodel/topic) and string tables. Thus, it is possible to transactionally move data from tables to topics and vice versa, as well as between topics, so that data is not lost or duplicated. Transactions can be enabled by the cluster administrator using the `enable_topic_service_tx` and `enable_pqconfig_transactions_at_scheme_shard` settings in the [dynamic configuration](./devops/configuration-management/configuration-v1/dynamic-config#obnovlenie-dinamicheskoj-konfiguracii).
* [Added](https://github.com/ydb-platform/ydb/pull/7150) support for [CDC](./concepts/cdc) for synchronous secondary indexes.
* Added the ability to change the retention period for records in [CDC](./concepts/cdc.md) topics.
* Added support for [auto-increment](./yql/reference/types/serial) for columns included in the primary key of a table.
* Added writing to the [audit log](./security/audit-log) of user login events in YDB, user session termination events in the user interface, as well as backup and restore requests.
* Added a system view that allows you to get information about sessions established with the database using a query.
* Added support for default constant values for columns of string tables.
* Added support for the `RETURNING` expression in queries.
* Added [built-in function](./yql/reference/builtins/basic.md#version) `version()`.
* [Added](https://github.com/ydb-platform/ydb/pull/8708) start/end time and author to the metadata of backup/restore operations from an S3-compatible storage.
* Added support for ACL backup/restore for tables from an S3-compatible storage.
* For queries reading from S3, paths and decompression method have been added to the plan.
* Added new parsing settings for `timestamp`, `datetime` when reading data from S3.
* Added support for the `Decimal` type in [partitioning keys](https://ydb.tech/docs/en/dev/primary-key/column-oriented#klyuch-particionirovaniya).
* Improved diagnosis of storage problems in HealthCheck.
* **_(Experimentally)_** Added [cost optimizer](./concepts/query_execution/optimizer#stoimostnoj-optimizator-zaprosov) for complex queries involving [columnar tables](./concepts/glossary#column-oriented-table). The optimizer considers a large number of alternative execution plans and selects the best one based on the cost estimate of each option. Currently, the optimizer works only with plans that include [JOIN](./yql/reference/syntax/join) operations.
* **_(Experimentally)_** Implemented an initial version of the [workload manager](./dev/resource-consumption-management), which allows you to create resource pools with limits on CPU, memory, and the number of active queries. Resource classifiers have been implemented to assign queries to a specific resource pool.
* **_(Experimentally)_** Implemented [automatic index selection](https://ydb.tech/docs/en/dev/secondary-indexes#avtomaticheskoe-ispolzovanie-indeksov-pri-vyborke) when executing a query, which can be enabled by the cluster administrator using the `index_auto_choose_mode` setting in `table_service_config` in the [dynamic configuration](./devops/configuration-management/configuration-v1/dynamic-config#obnovlenie-dinamicheskoj-konfiguracii).

#### YDB UI

* Supported creation and [displaying](https://github.com/ydb-platform/ydb-embedded-ui/issues/782) of an asynchronous replication instance.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/issues/929) designation for [auto-increment columns](./yql/reference/types/serial).
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1438) a tab with information about [tablets](./concepts/glossary#tablet).
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1289) a tab with information about [distributed storage groups](./concepts/glossary#storage-group).
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1218) a setting to add [tracing](./reference/observability/tracing/setup) to all queries and display the results of query tracing.
* The PDisk page has been added with [attributes](https://github.com/ydb-platform/ydb-embedded-ui/pull/1069), information about disk space consumption, and a button that launches [disk decommissioning](./devops/deployment-options/manual/decommissioning).
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1313) information about running queries.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1291) a setting for the row limit in the query editor output and display if the query results exceed the limit.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1049) display of a list of queries with the highest CPU consumption over the last hour.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1127) search on pages with query history and a list of saved queries.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1117) the ability to interrupt query execution.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/issues/944) the ability to save a query from the editor using hotkeys.
* [Separated](https://github.com/ydb-platform/ydb-embedded-ui/pull/1422) the display of disks from donor disks.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1154) support for InterruptInheritance ACL and improved display of active ACLs.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/889) display of the current version of the user interface.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1229) information about the state of experimental functionality enablement settings.

#### Performance

* [Accelerated](https://github.com/ydb-platform/ydb/pull/7589) recovery from backups of tables with secondary indexes by up to 20% according to our tests.
* [Optimized](https://github.com/ydb-platform/ydb/pull/9721) the throughput of Interconnect.
* Improved the performance of CDC topics containing thousands of partitions.
* Made a number of improvements to the Hive tablet balancing algorithm.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/6850) an error that rendered a database with a large number of tables or partitions inoperable when restoring from a backup. Now, if the database size limits are exceeded, the restore operation will fail, and the database will continue to operate normally.
* [Implemented](https://github.com/ydb-platform/ydb/pull/11532) a mechanism that forcibly triggers background [compaction](./concepts/glossary#compaction) when discrepancies are detected between the data schema and the data stored in [DataShard](./concepts/glossary#data-shard). This resolves a rarely occurring problem with delays in changing the data schema.
* [Eliminated](https://github.com/ydb-platform/ydb/pull/10447) duplication of authentication tickets, which led to an increased number of requests to authentication providers.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9377) an error violating the invariant during the initial CDC scan, which led to the crash of the ydbd server process.
* [Prohibited](https://github.com/ydb-platform/ydb/pull/9446) changing the schema of backup tables.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9509) the hanging of the initial CDC scan during frequent table updates.
* [Excluded](https://github.com/ydb-platform/ydb/pull/9934) deleted indexes from the count of the [maximum number of indexes](https://ydb.tech/docs/en/concepts/limits-ydb#schema-object).
* [Fixed](https://github.com/ydb-platform/ydb/pull/8847) [an error](https://github.com/ydb-platform/ydb/issues/6985) in the display of the time at which the execution of a set of transactions is scheduled (planned step).
* [Fixed](https://github.com/ydb-platform/ydb/pull/9161) [an issue](https://github.com/ydb-platform/ydb/issues/8942) with blue-green deployment interruption in large clusters due to frequent updates of the node list.
* [Fixed](https://github.com/ydb-platform/ydb/pull/8925) a rarely occurring error that led to a violation of the order of transaction execution.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9841) [an error](https://github.com/ydb-platform/ydb/issues/9797) in the EvWrite API that led to incorrect memory release.
* [Fixed](https://github.com/ydb-platform/ydb/pull/10698) [an issue](https://github.com/ydb-platform/ydb/issues/10674) with volatile transactions hanging after restart.
* Fixed an error in CDC that in some cases led to increased CPU usage, up to a core per CDC partition.
* [Eliminated](https://github.com/ydb-platform/ydb/pull/11061) the read delay occurring during and after the splitting of some partitions.
* Fixed errors when reading data from S3.
* [Fixed](https://github.com/ydb-platform/ydb/pull/4793) the method of calculating the aws signature when accessing S3.
* Fixed false positives of the HealthCheck system during the backup of a database with a large number of shards.

## Version 24.2 {#24-2}

Release date: August 20, 2024.

### Functionality

* Added the ability to [set priorities](./devops/deployment-options/manual/maintenance.md) for maintenance tasks in the [cluster management system](./concepts/glossary#cms).
* Added [stable names configuration](reference/configuration/node_broker_config.md#node-broker-config) for cluster nodes within a tenant.
* Added retrieval of nested groups from the [LDAP server](./security/authentication.md#ldap), improved host parsing in the [LDAP configuration](reference/configuration/auth_config.md#ldap-auth-config), and added an option to disable built-in authentication by login and password.
* Added the ability to authenticate [dynamic nodes](./concepts/glossary#dynamic) using an SSL certificate.
* Implemented the removal of inactive nodes from [Hive](./concepts/glossary#hive) without restarting it.
* Improved management of inflight pings when restarting Hive in large clusters.
* [Changed](https://github.com/ydb-platform/ydb/pull/6381) the order of establishing connections with nodes when restarting Hive.

### YDB UI

* [Added](https://github.com/ydb-platform/ydb/pull/7485) the ability to set a TTL for a user session in the configuration file.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1028) sorting by `CPUTime` in the table with the list of queries.
* [Fixed](https://github.com/ydb-platform/ydb/pull/7779) loss of precision when working with `double`, `float`.
* Supported [creating directories from the UI](https://github.com/ydb-platform/ydb-embedded-ui/issues/958).
* [Added the ability](https://github.com/ydb-platform/ydb-embedded-ui/pull/976) to set the interval for background data updates on all pages.
* [Improved](https://github.com/ydb-platform/ydb-embedded-ui/issues/955) ACL display.
* Enabled autocomplete in the query editor by default.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/834) support for View.

### Bug fixes

* Added a check for the size of a local transaction before committing it to fix [errors](https://github.com/ydb-platform/ydb/issues/6677) in the operation of schema operations when exporting/backing up large databases.
* [Fixed](https://github.com/ydb-platform/ydb/pull/7709) [error](https://github.com/ydb-platform/ydb/issues/7674) of duplicate results of the SELECT query when reducing the quota in [DataShard](./concepts/glossary#data-shard).
* [Fixed](https://github.com/ydb-platform/ydb/pull/6461) [errors](https://github.com/ydb-platform/ydb/issues/6220) occurring when changing the state of the [coordinator](./concepts/glossary#coordinator).
* [Fixed](https://github.com/ydb-platform/ydb/pull/5992) errors occurring during the initial scan of [CDC](./dev/cdc).
* [Fixed](https://github.com/ydb-platform/ydb/pull/6615) race condition in asynchronous change delivery (asynchronous indexes, CDC).
* [Fixed](https://github.com/ydb-platform/ydb/pull/5993) a rare error that caused the process to crash when deleting by [TTL](./concepts/ttl).
* [Fixed](https://github.com/ydb-platform/ydb/pull/5760) error in displaying the PDisk status in the [CMS](./concepts/glossary#cms) interface.
* [Fixed](https://github.com/ydb-platform/ydb/pull/6008) errors that could cause tablet drain from a node to hang.
* [Fixed](https://github.com/ydb-platform/ydb/pull/6445) error of stopping the interconnect proxy on a node running without restarts when adding another node to the cluster.
* [Fixed](https://github.com/ydb-platform/ydb/pull/6695) accounting of free memory in [interconnect](./concepts/glossary#actor-system-interconnect).
* [Fixed](https://github.com/ydb-platform/ydb/issues/6405) counters of UnreplicatedPhantoms/UnreplicatedNonPhantoms in VDisk.
* [Fixed](https://github.com/ydb-platform/ydb/issues/6398) handling of empty garbage collection requests on VDisk.
* [Fixed](https://github.com/ydb-platform/ydb/pull/5894) management of TVDiskControls settings via CMS.
* [Fixed](https://github.com/ydb-platform/ydb/pull/5883) error of loading data created by newer versions of VDisk.
* [Fixed](https://github.com/ydb-platform/ydb/pull/5862) error when executing a query `REPLACE INTO` with a default value.
* [Fixed](https://github.com/ydb-platform/ydb/pull/7714) error in executing queries that performed multiple left joins to the same string table.
* [Fixed](https://github.com/ydb-platform/ydb/pull/7740) loss of precision for `float`, `double` types when using CDC.

## Version 24.1 {#24-1}

Release date: July 31, 2024.

### Functionality

* Implemented [Knn UDF](./yql/reference/udf/list/knn.md) for precise search of the nearest vectors.
* Developed a gRPC QueryService that allows executing all types of queries (DML, DDL) and retrieving unlimited amounts of data.
* Implemented [integration with the LDAP protocol](./security/authentication.md) and the ability to obtain a list of groups from external LDAP directories.

### Built-in UI

* Added a resource consumption diagnostics dashboard, located on the database information tab, which helps determine the current state of resource consumption: CPU cores, RAM, and network distributed storage space.
* Added graphs for monitoring key cluster performance indicators {{ ydb-short-name }}.

### Performance

* [Optimized](https://github.com/ydb-platform/ydb/pull/1837) session timeouts for the coordination service from server to client. Previously, the timeout was 5 seconds, which in the worst case led to identifying a non-working client (and releasing the resources it held) within 10 seconds. In the new version, the check time depends on the session wait time, which ensures faster response when changing the leader or acquiring distributed locks.
* [Optimized](https://github.com/ydb-platform/ydb/pull/2391) CPU consumption by [SchemeShard](./concepts/glossary.md#scheme-shard) replicas, especially when processing fast updates for tables with a large number of partitions.

### Error fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/3917) potential queue overflow error; [Change Data Capture](./dev/cdc.md) reserves change queue capacity during the initial scan.
* [Fixed](https://github.com/ydb-platform/ydb/pull/4597) potential deadlock between obtaining CDC records and sending them.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2056) the issue of losing the mediator task queue upon reconnection, allowing the mediator task queue to be processed during resynchronization.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2624) a rare error where, with enabled and used volatile transactions, a successful transaction confirmation result was returned before the transaction was successfully committed. Volatile transactions are disabled by default and are under development.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2839) a rare error that led to the loss of established locks and the successful confirmation of transactions that should have resulted in a Transaction Locks Invalidated error.
* [Fixed](https://github.com/ydb-platform/ydb/pull/3074) a rare error that could lead to a possible violation of data integrity guarantees during concurrent write and read operations on a specific key.
* [Fixed](https://github.com/ydb-platform/ydb/pull/4343) an issue that caused read replicas to stop processing queries.
* [Fixed](https://github.com/ydb-platform/ydb/pull/4979) a rare error that could lead to database processes crashing when there were uncommitted transactions on a table at the time of its renaming.
* [Fixed](https://github.com/ydb-platform/ydb/pull/3632) an error in the logic for determining the status of a static group, where the static group was not marked as non-working when it should have been.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2169) an error of partial commit of a distributed transaction with uncommitted changes in case of some races with restarts.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2374) anomalies with reading outdated data that were [detected using Jepsen](https://blog.ydb.tech/hardening-ydb-with-jepsen-lessons-learned-e3238a7ef4f2).

## Version 23.4 {#23-4}

Release date: May 14, 2024.

### Performance

* [Fixed](https://github.com/ydb-platform/ydb/pull/3638) the issue of increased consumption of computing resources by the actor of topics `PERSQUEUE_PARTITION_ACTOR`.
* [Optimized](https://github.com/ydb-platform/ydb/pull/2083) resource usage by SchemeBoard replicas. The greatest effect is noticeable when modifying table metadata with a large number of partitions.

### Error fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/2169) an error of possible incomplete commit of accumulated changes when using distributed transactions. This error occurs with an extremely rare combination of events, including the restart of tablets serving the table partitions involved in the transaction.
* [Resolved](https://github.com/ydb-platform/ydb/pull/3165) a race condition between table merge processes and garbage collection, which could cause the garbage collection to fail with an invariant violation error and, as a result, an abnormal termination of the server process `ydbd`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2696) an error in Blob Storage where information about changes in the storage group composition might not reach certain cluster nodes in a timely manner. As a result, in rare cases, read and write operations on data in the affected group could be blocked, requiring manual administrator intervention.
* [Fixed](https://github.com/ydb-platform/ydb/pull/3002) an error in Blob Storage that could prevent data storage nodes from starting even with a correct configuration. The error occurred in systems with the "blob depot" experimental feature explicitly enabled (this feature is off by default).
* [Fixed](https://github.com/ydb-platform/ydb/pull/2475) an error that occurred in some situations when writing to a topic with an empty `producer_id` when deduplication was disabled. It could lead to an abnormal termination of the server process `ydbd`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2651) an issue that caused the `ydbd` process to crash due to an erroneous write session state in the topic.
* [Fixed](https://github.com/ydb-platform/ydb/pull/3587) a metric display error for the number of partitions in a topic; it previously displayed an incorrect value.
* [Resolved](https://github.com/ydb-platform/ydb/pull/2126) memory leaks that occurred when copying topic data between clusters {{ ydb-short-name }}. They could lead to server processes `ydbd` terminating due to running out of available RAM.

## Version 23.3 {#23-3}

Release date: October 12, 2023.

### Functionality

* Added visibility of own changes within transactions. Previously, attempting to read data already modified in the current transaction would result in an error. This required ordering reads and writes within the transaction. With the introduction of visibility of own changes, these restrictions are lifted, and queries can read rows modified in the current transaction.
* Added support for [columnar tables](concepts/datamodel/table.md#column-tables). Columnar tables are well-suited for analytical queries (Online Analytical Processing) because only the columns directly involved in the query are read. YDB columnar tables allow creating analytical reports with performance comparable to specialized analytical DBMS.
* Added support for [Kafka API for topics](reference/kafka-api/index.md). Now YDB topics can be accessed via Kafka-compatible API, which is intended for migrating existing applications. Support for Kafka protocol version 3.4.0 is provided.
* Added the ability to [write to a topic without deduplication](concepts/datamodel/topic.md#no-dedup). This type of writing is well-suited for cases where the order of message processing is not critical. Writing without deduplication is faster and consumes fewer server resources, but no ordering or deduplication of messages is performed on the server.
* YQL has added the capabilities to [create](yql/reference/syntax/create-topic.md), [modify](yql/reference/syntax/alter-topic.md), and [delete](yql/reference/syntax/drop-topic.md) topics.
* Added the ability to grant and revoke access rights using YQL commands [GRANT](yql/reference/syntax/grant.md) and [REVOKE](yql/reference/syntax/revoke.md).
* Added the ability to log DML operations in the audit log.
* **_(Experimental)_** When writing messages to a topic, it is now possible to pass metadata. To enable this functionality, add `enable_topic_message_meta: true` to the [configuration file](reference/configuration/index.md).
* **_(Experimental)_** Added the ability to [read from topics](reference/ydb-sdk/topic.md#read-tx) and write to a table within a single transaction. This new feature simplifies the scenario of transferring data from a topic to a table. To enable it, add `enable_topic_service_tx: true` to the configuration file.
* **_(Experimental)_** Added support for PostgreSQL compatibility. The new mechanism allows executing SQL queries in PostgreSQL dialect on YDB infrastructure using the PostgreSQL network protocol. You can use familiar PostgreSQL tools such as psql and drivers (pq for Golang and psycopg2 for Python), as well as develop queries using PostgreSQL syntax with YDB's horizontal scalability and fault tolerance.
* **_(Experimental)_** Added support for [federated queries](concepts/query_execution/federated_query/index.md). It allows retrieving information from various data sources without moving data to YDB. Interaction with ClickHouse, PostgreSQL, and S3 is supported via YQL queries without duplicating data between systems.

### Embedded UI

* A new option `PostgreSQL` has been added to the query type selector settings, which is available when the `Enable additional query modes` parameter is enabled. Also, the query history now takes into account the syntax used when executing the query.
* The YQL query template for creating a table has been updated. Added a description of the available parameters.
* Sorting and filtering for Storage and Nodes tables have been moved to the server. To use this functionality, you need to enable the `Offload tables filters and sorting to backend` parameter in the experiments section.
* Buttons for creating, modifying, and deleting [topics](concepts/datamodel/topic.md) have been added to the context menu.
* Added sorting by criticality for all issues in the tree in `Healthcheck`.

### Performance

* Implemented iterator reads. This new functionality allows separating reads and computations. Iterator reads enable datashards to increase the throughput of read queries.
* Optimized the performance of writing to YDB topics.
* Improved tablet balancing when nodes are overloaded.

### Bug fixes

* Fixed an error regarding the possible blocking of snapshots by reading iterators that coordinators are not aware of.
* Fixed a memory leak when closing a connection in Kafka proxy.
* Fixed an issue where snapshots taken through reading iterators may not recover on restarts.
* Fixed an incorrect residual predicate for the `IS NULL` condition on a column.
* Fixed the triggering of the `VERIFY failed: SendResult(): requirement ChunksLimiter.Take(sendBytes) failed` check.
* Fixed `ALTER TABLE` for columnar tables `TTL`.
* Implemented `FeatureFlag`, which allows enabling/disabling work with `CS` and `DS`.
* Fixed the 50 ms time difference between coordinator times in 23-2 and 23-3.
* Fixed an error where the `storage` endpoint returned extra groups when the `node_id` parameter was present in the request `viewer backend`.
* Added a `usage` filter to `/storage` in `viewer backend`.
* Fixed an error in Storage v2 where an incorrect number was returned in `Degraded`.
* Fixed the cancellation of subscriptions from sessions in iterator reads during tablet restarts.
* Fixed an error where `healthcheck` alerts about storage flickered during rolling restarts when going through a load balancer.
* Updated `cpu usage` metrics in YDB.
* Fixed the ignoring of `NULL` when specifying `NOT NULL` in the table schema.
* Implemented logging of `DDL` operations in the common log.
* Implemented a restriction for the `ydb table attribute add/drop` command to work only with tables and not with other objects.
* Disabled `CloseOnIdle` for `interconnect`.
* Fixed the doubling of read speed in the UI.
* Fixed an error where data could be lost on `block-4-2`.
* Added a topic name validation check.
* Fixed a possible `deadlock` in the actor system.
* Fixed the `KqpScanArrowInChanels::AllTypesColumns` test.
* Fixed the `KqpScan::SqlInParameter` test.
* Fixed concurrency issues for OLAP queries.
* Fixed the insertion of `ClickBench parquet`.
* Added a missing call to `CheckChangesQueueOverflow` in the general `CheckDataTxReject`.
* Fixed an error that returned an empty status in calls to `ReadRows API`.
* Fixed incorrect retries in the final stage of export.
* Fixed an issue with an infinite quota for the number of records in a CDC topic.
* Fixed an error importing `string` and `parquet` columns into an `string` OLAP column.
* Fixed a crash of `KqpOlapTypes.Timestamp` under tsan.
* Fixed a crash in `viewer backend` when attempting to execute a query to the database due to version incompatibility.
* Fixed an error where `viewer` did not return a response from `healthcheck` due to a timeout.
* Fixed an error where incorrect `ExpectedSerial` values could be saved in Pdisks.
* Fixed an error where database nodes crashed due to `segfault` in the S3 actor.
* Fixed a race condition in `ThreadSanitizer: data race KqpService::ToDictCache-UseCache`.
* Fixed a race condition in `GetNextReadId`.
* Fixed an inflated result in `SELECT COUNT(*)` immediately after import.
* Fixed an error where `TEvScan` could return an empty dataset in the case of shard splitting.
* Added a separate issue/error code for cases of exhausted available space.
* Fixed error `GRPC_LIBRARY Assertion failed`.
* Fixed an error where scanning queries on secondary indexes returned an empty result.
* Fixed validation of `CommitOffset` in `TopicAPI`.
* Reduced `shared cache` consumption when approaching OOM.
* Merged the logic of schedulers from `data executer` and `scan executer` into one class.
* Added `discovery` and `proxy` handlers to the query execution process in `query` in `viewer backend`.
* Fixed an error where the `/cluster` endpoint returns the name of the root domain of type `/ru` in `viewer backend`.
* Implemented a seamless update scheme for `QueryService` tablets.
* Fixed an error where `DELETE` returned data but did not delete it.
* Fixed an error in the operation of `DELETE ON` in `query service`.
* Fixed unexpected disabling of batching in default schema settings.
* Fixed the triggering of the `VERIFY failed: MoveUserTable(): requirement move.ReMapIndexesSize() == newTableInfo->Indexes.size()` check.
* Increased the default timeout for grpc-streaming.
* Excluded unused messages and methods from `QueryService`.
* Added sorting by `Rack` in `/nodes` in `viewer backend`.
* Fixed an error where a query with sorting returned an error in descending order.
* Improved interaction between `QP` and `NodeWhiteboard`.
* Removed support for old parameter formats.
* Fixed an error where `DefineBox` was not applied to disks with a static group.
* Fixed error `SIGSEGV` in dinnodes when importing `CSV` via `YDB CLI`.
* Fixed an error causing a crash when processing `NGRpcService::TRefreshTokenImpl`.
* Implemented the `gossip` protocol for exchanging information about cluster resources.
* Fixed error `DeserializeValuePickleV1(): requirement data.GetTransportVersion() == (ui32) NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0 failed`.
* Implemented auto-increment columns.
* Use `UNAVAILABLE` status instead of `GENERIC_ERROR` when shard identification fails.
* Added support for `rope payload` in `TEvVGet`.
* Added ignoring of outdated events.
* Fixed a crash of write sessions on an invalid topic name.
* Fixed error `CheckExpected(): requirement newConstr failed, message: Rewrite error, missing Distinct((id)) constraint in node FlatMap`.
* Enabled `safe heal` by default.

## Version 23.2 {#23-2}

Release date: August 14, 2023.

### Functionality

* **_(Experimental)_** Implemented visibility of own changes. When this feature is enabled, you can read modified values from the current transaction that has not yet been committed. This functionality also allows performing multiple modifying operations in one transaction on a table with secondary indexes. To enable this feature, add `enable_kqp_immediate_effects: true` to the `table_service_config` section in the [configuration file](reference/configuration/index.md).
* **_(Experimental)_** Implemented iterator reads. This functionality allows separating reads and computations. Iterator reads enable datashards to increase the throughput of read queries. To enable this feature, add `enable_kqp_data_query_source_read: true` to the `table_service_config` section in the [configuration file](reference/configuration/index.md).

### Embedded UI

* Improved navigation:
  * The buttons for switching between diagnostic and development modes have been moved to the left panel.
  * Breadcrumbs have been added to all pages.
  * On the database page, information about storage groups and database nodes has been moved to tabs.
* Query history and saved queries have been moved to tabs above the query editor.
* In the Info tabs for schema objects, settings are displayed in terms of `CREATE` or `ALTER` constructs.
* Support for displaying [columnar tables](concepts/datamodel/table.md#column-table) in the schema tree has been added.

### Performance

* For scanning queries, the ability to efficiently search for individual rows using a primary key or secondary indexes has been implemented. This can significantly improve performance in many cases. As with regular queries, to use a secondary index, you must explicitly specify its name in the query text using the `VIEW` keyword.

* **_(Experimentally)_** The ability to manage system tablets of the database (SchemeShard, Coordinators, Mediators, SysViewProcessor) with its own Hive, instead of the root Hive, has been added, and this can be done immediately at the time of creating a new database. Without this flag, the system tablets of the new database are created in the root Hive, which can negatively affect its load. Enabling this flag makes databases completely isolated in terms of load, which can be especially important for installations consisting of a hundred or more nodes. To enable this functionality, add `alter_database_create_hive_first: true` to the `feature_flags` section in the [configuration file](reference/configuration/index.md).

### Bug fixes

* A bug in the auto-configuration of the actor system has been fixed, as a result of which all the load falls on the system pool.
* A bug that leads to a full scan when searching by the prefix of the primary key through `LIKE` has been fixed.
* Bugs in interaction with datashard replicas have been fixed.
* Bugs in working with memory in columnar tables have been fixed.
* Bugs in processing conditions for immediate transactions have been fixed.
* A bug in the operation of iterator reads on datashard replicas has been fixed.
* A bug that leads to an avalanche-like reinstallation of data delivery sessions to asynchronous indexes has been fixed.
* Bugs in the optimizer in scanning queries have been fixed.
* A bug in the incorrect calculation of hive storage consumption after database expansion has been fixed.
* A bug with the hanging of operations from non-existent iterators has been fixed.
* Bugs in reading a range on the `NOT NULL` column have been fixed.
* A bug with the hanging of VDisk replication has been fixed.
* A bug in the operation of the `run_interval` option in TTL has been fixed.

## Version 23.1 {#23-1}

Release date: May 5, 2023. To update to version 23.1, go to the [Downloads](downloads/index.md#ydb-server) section.

### Functionality

* [Initial table scanning](concepts/cdc.md#initial-scan) when creating a CDC change stream has been added. Now it is possible to download all the data that exists at the time of stream creation.
* The ability to [atomically replace an index](dev/secondary-indexes.md#atomic-index-replacement) has been added. Now it is possible to atomically and transparently for the application replace one index with another pre-created index. The replacement is performed without downtime.
* [Audit log](security/audit-log.md) has been added — a stream of events that contains information about all operations on {{ ydb-short-name }} objects.

### Performance

* Data transfer formats between query execution stages have been improved, which has sped up SELECT on queries with parameters by 10%, and on write operations — up to 30%.
* [Automatic configuration](reference/configuration/index.md) of actor system pools depending on their load has been added. This improves performance through more efficient sharing of CPU resources.
* The logic of applying predicates has been optimized — the implementation of restrictions using OR and IN with parameters is automatically moved to the DataShard side.
* (Experimentally) For scanning queries, the ability to efficiently search for individual rows using the primary key or secondary indexes has been implemented, which can significantly improve performance in many cases. As in regular queries, to use a secondary index, you must explicitly specify its name in the query text using the `VIEW` keyword.
* Caching of the computation graph during query execution has been implemented, which reduces CPU consumption when building it.

### Bug fixes

* A number of bugs in the implementation of the distributed data storage have been fixed. We strongly recommend that all users update to the latest version.
* The error in building the index on not null columns has been fixed.
* The statistics calculation with MVCC enabled has been fixed.
* Backup errors have been fixed.
* The race condition during table split and deletion with CDC has been fixed.

## Version 22.5 {#22-5}

Release date: March 7, 2023. To update to version **22.5**, go to the [Downloads](downloads/index.md#ydb-server) section.

### What's new

* Configuration parameters for the change stream [change stream configuration parameters](yql/reference/syntax/alter_table/changefeed.md) have been added to pass additional information about changes to the topic.
* Support for [renaming tables](concepts/datamodel/table.md#rename) with TTL enabled has been added.
* [Management of record retention time](concepts/cdc.md#retention-period) for the change stream has been added.

### Bug fixes and improvements

* The error when inserting 0 rows with the BulkUpsert operation has been fixed.
* The error when importing Date/DateTime columns from CSV has been fixed.
* The error in importing data from CSV with a line break has been fixed.
* The error in importing data from CSV with empty values has been fixed.
* Query Processing performance has been improved (WorkerActor has been replaced with SessionActor).
* DataShard compaction now starts immediately after split or merge operations.

## Version 22.4 {#22-4}

Release date: October 12, 2022. To update to version **22.4**, go to the [Downloads](downloads/index.md#ydb-server) section.

### What's new

* {{ ydb-short-name }} Topics and Change Data Capture (CDC):

  * A new Topic API has been introduced. [Topic](concepts/datamodel/topic.md) {{ ydb-short-name }} is an entity for storing unstructured messages and delivering them to various subscribers.
  * Support for the new Topic API has been added to [{{ ydb-short-name }} CLI](reference/ydb-cli/topic-overview.md) and [SDK](reference/ydb-sdk/topic.md). The Topic API provides methods for streaming writing and reading messages, as well as managing topics.
  * The ability to [capture table data changes](concepts/cdc.md) and send change messages to a topic has been added.

* SDK:

  * The ability to interact with topics in {{ ydb-short-name }} SDK has been added.
  * Official support for the database/sql driver for working with {{ ydb-short-name }} in Golang has been added.

* Embedded UI:

  * The CDC change stream and secondary indexes are now displayed in the database schema hierarchy as separate objects.
  * The visualization of the graphical representation of query explain plans has been improved.
  * Problematic storage groups are now more noticeable.
  * Various improvements based on UX research have been made.

* Query Processing:

  * The Query Processor 2.0, a new subsystem for executing OLTP queries with significant improvements over the previous version, has been added.
  * Write performance has improved by up to 60%, read performance by up to 10%.
  * The ability to enable NOT NULL constraints for primary keys in YDB when creating tables has been added.
  * Support for renaming a secondary index online without stopping the service has been added.
  * The query explain representation has been improved and now includes graphs for physical operators.

* Core:

  * Support for a consistent snapshot that does not conflict with writing transactions has been added for read-only transactions.
  * Support for BulkUpsert for tables with asynchronous secondary indexes has been added.
  * Support for TTL for tables with asynchronous secondary indexes has been added.
  * Support for compression when exporting data to S3 has been added.
  * Audit log for DDL statements has been added.
  * Authentication with static credentials has been supported.
  * System views for query performance diagnostics have been added.
