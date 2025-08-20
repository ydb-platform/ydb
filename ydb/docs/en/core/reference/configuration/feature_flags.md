# Feature Flags

The `feature_flags` configuration section enables or disables specific {{ ydb-short-name }} features using boolean flags. These flags control experimental features, performance optimizations, and compatibility settings that can be toggled without changing the {{ ydb-short-name }} code. For general information about {{ ydb-short-name }} configuration, see [{#T}](../../devops/configuration-management/index.md).

{% note warning %}

There's always a reason for a feature to be disabled by default. It might be a new feature that is not confirmed to be production-ready yet, an experimental feature, a deprecated feature, or a feature for a niche use case with caveats. Thus, this configuration section is intended mostly for testing purposes.

**Manually enable {{ ydb-short-name }} features in production environments only at your own risk.**

{% endnote %}


## Available Settings

The following sections list all available feature flags, their default values, restart requirements, and detailed descriptions. Flags marked as "Deprecated" are maintained for backward compatibility and should not be used in new deployments.

### enable_separate_solomon_shard_for_pdisk

**Description:** Uses a dedicated monitoring shard for [PDisk](../../concepts/glossary.md#pdisk) metrics collection, improving monitoring performance and reducing interference with storage operations. This isolates Solomon (monitoring system) metrics from main PDisk functionality.

**Default:** `true`.

**Requires restart:** no.

### use_forseti_scheduler_by_default_for_pdisk

**Description:** Enables the Forseti I/O scheduler for PDisk operations, which provides better fairness and latency control for disk operations. Forseti is {{ ydb-short-name }}'s custom disk scheduler optimized for database workloads.

**Default:** `true`.

**Requires restart:** no.

### enable_separate_trim_thread_for_pdisk

**Description:** Runs SSD TRIM operations in a dedicated thread, preventing TRIM commands from blocking other PDisk I/O operations. This improves performance on SSDs by allowing garbage collection without affecting read/write latency.

**Default:** `true`.

**Requires restart:** no.

### enable_separate_submit_thread_for_pdisk

**Description:** Processes I/O request submissions in a separate thread, reducing latency and improving throughput by preventing submission operations from blocking the main PDisk processing loop.

**Default:** `true`.

**Requires restart:** no.

### enable_per_owner_chunk_limit_for_pdisk

**Description:** Enforces individual chunk allocation limits per [VDisk](../../concepts/glossary.md#vdisk) owner on PDisk, preventing any single owner from monopolizing storage space. This provides better resource isolation between different database components.

**Default:** `false`.

**Requires restart:** no.

### trim_entire_device_on_startup

**Description:** Performs a full device TRIM operation during PDisk startup to ensure optimal SSD performance. Warning: This can significantly increase startup time on large devices.

**Default:** `false`.

**Requires restart:** no.

### enable_chunk_grace_for_pdisk

**Description:** Allows temporary exceeding of chunk limits under certain conditions, providing flexibility during high-load scenarios while maintaining overall resource controls.

**Default:** `true`.

**Requires restart:** no.

### allow_consistent_operations_for_schemeshard

**Description:** Enables consistent schema operations in [SchemeShard](../../concepts/glossary.md#scheme-shard), ensuring that schema changes are applied atomically across all [nodes](../../concepts/glossary.md#node). This provides stronger consistency guarantees for DDL operations. For more about consistency models, see [{#T}](../../concepts/transactions.md).

**Default:** `true`.

**Requires restart:** no.


### enable_scheme_board

**Description:** Deprecated; always enabled.

**Default:** `true`.

**Requires restart:** no.


### allow_ydb_requests_without_database

**Description:** Allows API requests to be processed without explicitly specifying a [database](../../concepts/glossary.md#database) path. When disabled, all requests must include a database identifier for improved security and multi-tenancy support.

**Default:** `true`.

**Requires restart:** yes.


### enable_external_subdomains

**Description:** Enables support for external subdomain configurations, allowing databases to be managed by external [Hive](../../concepts/glossary.md#hive) instances. This supports advanced multi-[cluster](../../concepts/glossary.md#cluster) and federation scenarios.

**Default:** `true`.

**Requires restart:** no.


### allow_recursive_mk_dir

**Description:** Deprecated; always enabled.

**Default:** `true`.

**Requires restart:** no.


### allow_huge_key_value_deletes

**Description:** Allows deletion of large numbers of key-value pairs in a single request. When disabled, large delete operations are limited to prevent performance impact and resource exhaustion.

**Default:** `true`.

**Requires restart:** no.


### send_schema_version_to_datashard

**Description:** Deprecated; always enabled.

**Default:** `true`.

**Requires restart:** no.


### enable_scheme_board_cache

**Description:** Deprecated; always enabled.

**Default:** `true`.

**Requires restart:** no.


### enable_system_views

**Description:** Enables system views that provide metadata and runtime information about the database state. See [system views documentation](../../dev/system-views.md) for available views and their usage.

**Default:** `true`.

**Requires restart:** yes.


### enable_external_hive

**Description:** Enables support for external Hive instances to manage tablet allocation and lifecycle. This allows for more flexible cluster topologies and advanced deployment scenarios.

**Default:** `true`.

**Requires restart:** no.


### use_scheme_board_cache_for_scheme_requests

**Description:** Deprecated; always enabled.

**Default:** `true`.

**Requires restart:** no.


### compile_minikql_with_version

**Description:** Deprecated; always enabled.

**Default:** `true`.

**Requires restart:** no.


### read_table_with_snapshot

**Description:** Deprecated; always enabled.

**Default:** `true`.

**Requires restart:** no.


### important_tablets_use_system_pool

**Description:** Runs critical system [tablets](../../concepts/glossary.md#tablet) (like SchemeShard, Hive) in a dedicated System thread pool, isolating them from user workload processing and ensuring system operations remain responsive under high load.

**Default:** `true`.

**Requires restart:** yes.


### enable_offline_slaves

**Description:** Deprecated; always enabled.

**Default:** `true`.

**Requires restart:** no.


### check_database_access_permission

**Description:** Enforces explicit access control checks for database operations. When enabled, users must have explicit permissions to access databases, improving security in multi-tenant environments. For details on access control, see [{#T}](../../security/authorization.md).

**Default:** `false`.

**Requires restart:** no.


### allow_online_index_build

**Description:** Deprecated; always enabled.

**Default:** `true`.

**Requires restart:** no.


### enable_persistent_query_stats

**Description:** Enables collection and storage of persistent query execution statistics for performance monitoring and optimization analysis. This provides detailed metrics about query execution patterns.

**Default:** `true`.

**Requires restart:** yes.


### disable_datashard_barrier

**Description:** Disables [DataShard](../../concepts/glossary.md#data-shard) synchronization barriers that ensure transaction ordering. Warning: This can improve performance but may violate consistency guarantees in some scenarios.

**Default:** `false`.

**Requires restart:** no.


### enable_put_batching_for_blob_storage

**Description:** Enables batching of PUT operations in [Distributed Storage](../../concepts/glossary.md#distributed-storage) to improve throughput by combining multiple small writes into larger, more efficient operations.

**Default:** `true`.

**Requires restart:** no.


### enable_kqp_wide_flow

**Description:** Deprecated; always enabled.

**Default:** `true`.

**Requires restart:** no.


### enable_kqp_scan_queries

**Description:** Deprecated; always enabled.

**Default:** `true`.

**Requires restart:** no.


### enable_persistent_partition_stats

**Description:** Enables collection and storage of detailed partition-level statistics for monitoring data distribution and access patterns. This helps with performance optimization and troubleshooting.

**Default:** `false`.

**Requires restart:** no.


### enable_ttl_on_indexed_tables

**Description:** Deprecated; always enabled.

**Default:** `true`.

**Requires restart:** no.


### allow_update_channels_binding_of_solomon_partitions

**Description:** Allows runtime updates to channel bindings for Solomon monitoring partitions, enabling dynamic reconfiguration of monitoring data flow without service restart.

**Default:** `false`.

**Requires restart:** no.


### disable_legacy_yql

**Description:** Deprecated; always enabled.

**Default:** `true`.

**Requires restart:** no.


### enable_data_column_for_index_table

**Description:** Enables the `DATA` column in secondary index tables, allowing indexes to store additional data columns for covering index functionality. See [secondary indexes documentation](../../dev/secondary-indexes.md) for details.

**Default:** `true`.

**Requires restart:** no.


### allow_serverless_storage_billing_for_schemeshard

**Description:** Enables serverless-style storage billing calculations in SchemeShard, allowing for pay-per-use pricing models in serverless database deployments.

**Default:** `false`.

**Requires restart:** no.


### enable_graceful_shutdown

**Description:** Enables graceful shutdown procedures that properly complete in-flight transactions and drain connections before stopping the service, preventing data loss and connection errors.

**Default:** `true`.

**Requires restart:** no.


### enable_drain_on_shutdown

**Description:** Ensures all in-flight operations complete before shutdown, working together with graceful shutdown to prevent data loss and maintain consistency during service restarts.

**Default:** `true`.

**Requires restart:** no.


### enable_configuration_cache

**Description:** Enables caching of configuration data to reduce configuration lookups and improve performance. Changes to this flag require a service restart to take effect.

**Default:** `false`.

**Requires restart:** yes.


### enable_db_counters

**Description:** Enables collection of database-level performance counters and metrics for monitoring and observability. This provides detailed insights into database operation performance.

**Default:** `false`.

**Requires restart:** yes.


### enable_clock_gettime_for_user_cpu_accounting

**Description:** Uses the `clock_gettime` system call for more precise user CPU time accounting, providing better resource usage tracking at the cost of slight performance overhead.

**Default:** `false`.

**Requires restart:** no.


### enable_async_indexes

**Description:** Deprecated; always enabled.

**Default:** `true`.

**Requires restart:** no.


### allow_stream_execute_yql_script

**Description:** Enables the streaming execution API for YQL scripts, allowing scripts to return results in chunks rather than waiting for complete execution. This improves memory usage for large result sets.

**Default:** `true`.

**Requires restart:** no.


### enable_kqp_scan_over_persistent_snapshot

**Description:** Deprecated; always enabled.

**Default:** `true`.

**Requires restart:** no.


### enable_olap_schema_operations

**Description:** Enables schema operations on OLAP (column-oriented) tables, allowing DDL operations like column addition, deletion, and modification on analytical workload tables.

**Default:** `true`.

**Requires restart:** no.


### enable_vpatch

**Description:** Enables VPatch (Virtual Patch) functionality for efficient incremental data updates, reducing network traffic and storage overhead for large data modifications.

**Default:** `true`.

**Requires restart:** no.


### enable_mvcc_snapshot_reads

**Description:** Deprecated; always enabled.

**Default:** `true`.

**Requires restart:** no.


### enable_mvcc

**Description:** Deprecated; [MVCC](../../concepts/mvcc.md) is always enabled.

**Default:** `true`.

**Requires restart:** no.


### enable_scheme_transactions_at_schemeshard

**Description:** Enables transactional schema operations at SchemeShard level, allowing multiple DDL operations to be executed atomically as a single transaction for better consistency.

**Default:** `true`.

**Requires restart:** no.


### enable_arrow_format_at_datashard

**Description:** Enables [Apache Arrow](https://arrow.apache.org/) columnar data format support at DataShard level for improved analytical query performance and better memory efficiency with columnar data processing.

**Default:** `false`.

**Requires restart:** no.


### enable_3x3_requests_for_mirror3dc_min_latency_put

**Description:** Enables optimized 3x3 request pattern for `mirror-3-dc` cluster topology to achieve minimum latency for PUT operations in geo-distributed deployments.

**Default:** `false`.

**Requires restart:** no.


### enable_background_compaction

**Description:** Enables background data compaction processes that optimize storage layout and improve read performance by merging and reorganizing data structures during idle periods.

**Default:** `true`.

**Requires restart:** no.


### enable_arrow_format_in_channels

**Description:** Enables Apache Arrow format for data transmission in internal communication channels, improving serialization performance and reducing memory overhead for large data transfers.

**Default:** `false`.

**Requires restart:** no.


### enable_background_compaction_serverless

**Description:** Enables background compaction specifically for Serverless database deployments, allowing storage optimization while maintaining the pay-per-use billing model constraints.

**Default:** `false`.

**Requires restart:** no.


### enable_not_null_columns

**Description:** Enables support for `NOT NULL` column constraints in table schemas, ensuring data integrity by preventing null values in specified columns during insert and update operations.

**Default:** `true`.

**Requires restart:** no.


### enable_ttl_on_async_indexed_tables

**Description:** Deprecated; always enabled.

**Default:** `true`.

**Requires restart:** no.


### enable_bulk_upsert_to_async_indexed_tables

**Description:** Enables efficient bulk upsert operations on tables with asynchronous secondary indexes, allowing high-performance batch data loading while maintaining index consistency.

**Default:** `true`.

**Requires restart:** no.


### enable_node_broker_single_domain_mode

**Description:** Deprecated; always enabled.

**Default:** `true`.

**Requires restart:** no.


### enable_public_api_external_blobs

**Description:** Allows the public API to handle external blob references, enabling storage of large binary objects outside the main database while maintaining referential integrity.

**Default:** `false`.

**Requires restart:** no.


### enable_public_api_keep_in_memory

**Description:** Allows the public API to use keep-in-memory optimization for frequently accessed data, improving query performance by caching hot data in memory at the cost of increased memory usage.

**Default:** `false`.

**Requires restart:** no.


### enable_implicit_scan_query_in_scripts

**Description:** Allows YQL scripts to automatically convert certain `SELECT` operations to scan queries when appropriate, improving performance for analytical workloads by leveraging scan-based execution.

**Default:** `true`.

**Requires restart:** no.


### allow_vdisk_defrag

**Description:** Enables VDisk defragmentation processes that optimize storage layout by reorganizing fragmented data blocks, improving storage efficiency and read performance over time.

**Default:** `true`.

**Requires restart:** no.


### enable_async_http_mon

**Description:** Deprecated; always enabled.

**Default:** `true`.

**Requires restart:** no.


### enable_changefeeds

**Description:** Enables [changefeeds](../../concepts/glossary.md#changefeed) functionality for tracking data changes in real-time. See [changefeeds documentation](../../dev/cdc.md) for streaming data changes to external systems.

**Default:** `true`.

**Requires restart:** no.


### enable_kqp_scan_query_multiple_olap_shards_reads

**Description:** Enables the query processor to execute scan queries across multiple OLAP shards in parallel, improving analytical query performance for large datasets.

**Default:** `false`.

**Requires restart:** no.


### enable_move_index

**Description:** Enables the ability to move secondary indexes between different storage groups or locations without rebuilding, allowing for flexible storage management and load balancing.

**Default:** `true`.

**Requires restart:** no.


### enable_failure_injection_termination

**Description:** Enables HTTP endpoint for controlled self-termination, used for chaos engineering and failure injection testing to simulate node failures in development and testing environments.

**Default:** `false`.

**Requires restart:** no.


### enable_chunk_locking

**Description:** Enables fine-grained locking mechanisms for data chunks in BlobStorage, improving concurrency control and reducing contention in high-write scenarios at the cost of increased locking overhead.

**Default:** `false`.

**Requires restart:** no.


### enable_not_null_data_columns

**Description:** Enables support for NOT NULL constraints specifically on data columns (non-key columns), extending data integrity enforcement beyond primary key columns to all table columns.

**Default:** `true`.

**Requires restart:** no.


### enable_grpc_audit

**Description:** Enables comprehensive audit logging for all gRPC API calls, recording request details, authentication information, and response codes for security monitoring and compliance purposes. For audit configuration details, see [{#T}](../../security/audit-log.md).

**Default:** `false`.

**Requires restart:** no.


### enable_borrowed_split_compaction

**Description:** Enables borrowed split compaction optimization that allows using resources from other partitions during compaction operations, improving overall system efficiency and reducing compaction latency.

**Default:** `true`.

**Requires restart:** no.


### enable_changefeed_initial_scan

**Description:** Enables initial table scan when creating changefeeds, allowing capture of existing data before tracking new changes. This provides complete historical context for data synchronization.

**Default:** `true`.

**Requires restart:** no.


### enable_dynamic_node_authorization

**Description:** Enables dynamic authorization for cluster nodes, allowing runtime updates to node permissions and access control without requiring cluster restart for security changes.

**Default:** `false`.

**Requires restart:** no.


### enable_data_shard_generic_read_sets

**Description:** Enables generic read-set functionality in DataShard for improved distributed transaction coordination, allowing more flexible transaction processing across multiple shards.

**Default:** `false`.

**Requires restart:** no.


### enable_alter_database_create_hive_first

**Description:** Changes the order of operations during `ALTER DATABASE` to create subdomain tablets in the subdomain's Hive first, improving database modification reliability and consistency.

**Default:** `true`.

**Requires restart:** no.


### enable_small_disk_optimization

**Description:** Enables optimizations specifically designed for small disk deployments, including adjusted buffer sizes, compaction strategies, and I/O patterns to improve performance on resource-constrained storage.

**Default:** `true`.

**Requires restart:** no.


### enable_datashard_volatile_transactions

**Description:** Enables volatile transactions in DataShard that offer improved performance for certain workloads by reducing transaction overhead, suitable for operations that can tolerate slightly relaxed durability guarantees.

**Default:** `true`.

**Requires restart:** no.


### enable_topic_service_tx

**Description:** Enables transactional support in the Topic service, allowing atomic operations across topic partitions and ensuring exactly-once message delivery semantics. See [topic concepts](../../concepts/topic.md) for details.

**Default:** `true`.

**Requires restart:** no.


### enable_llvm_cache

**Description:** Enables caching of LLVM-compiled query execution code, improving performance for repeated query patterns by avoiding recompilation overhead at the cost of increased memory usage.

**Default:** `false`.

**Requires restart:** no.


### enable_external_data_sources

**Description:** Enables support for querying external data sources directly from {{ ydb-short-name }}, allowing federated queries across different storage systems without data migration. This provides unified access to heterogeneous data sources.

**Default:** `false`.

**Requires restart:** no.


### enable_topic_disk_sub_domain_quota

**Description:** Enforces disk space quotas per subdomain for topic data, preventing any single subdomain from consuming excessive storage and ensuring fair resource allocation in multi-tenant environments.

**Default:** `true`.

**Requires restart:** yes.


### enable_separation_compute_actors_from_read

**Description:** Separates compute processing actors from the data read path, improving query performance by reducing contention and allowing better resource allocation between computation and I/O operations.

**Default:** `true`.

**Requires restart:** no.


### enable_pq_config_transactions_at_schemeshard

**Description:** Enables transactional configuration changes for PersistentQueue (PQ) at SchemeShard level, ensuring atomic updates to topic configuration and metadata.

**Default:** `true`.

**Requires restart:** no.


### enable_script_execution_operations

**Description:** Enables the script execution operations API, allowing management and monitoring of long-running YQL script executions with operation tracking and status queries.

**Default:** `true`.

**Requires restart:** no.


### enable_implicit_query_parameter_types

**Description:** Enables automatic type inference for query parameters when types are not explicitly specified, improving usability by reducing the need for explicit type declarations in parameterized queries.

**Default:** `true`.

**Requires restart:** no.


### enable_force_immediate_effects_execution

**Description:** Forces immediate execution of query effects instead of batching them, providing stronger consistency guarantees at the cost of reduced throughput for bulk operations.

**Default:** `false`.

**Requires restart:** no.


### enable_topic_split_merge

**Description:** Enables dynamic splitting and merging of topic partitions for load balancing and resource optimization, allowing topics to automatically adapt to changing throughput patterns.

**Default:** `true`.

**Requires restart:** no.


### enable_changefeed_dynamodb_streams_format

**Description:** Enables DynamoDB Streams-compatible format for changefeeds, facilitating integration with applications and tools designed for AWS DynamoDB Streams API.

**Default:** `true`.

**Requires restart:** no.


### force_column_tables_composite_marks

**Description:** Forces the use of composite marks in column tables for improved compression and query performance, trading some write performance for better storage efficiency and analytical query speed.

**Default:** `false`.

**Requires restart:** no.


### enable_subscriptions_in_discovery

**Description:** Enables subscription-based notifications in the discovery service, allowing clients to receive real-time updates about cluster topology changes and service availability.

**Default:** `true`.

**Requires restart:** yes.


### enable_get_node_labels

**Description:** Enables retrieval of node labels through the API, allowing applications to query node metadata for placement decisions and monitoring purposes.

**Default:** `false`.

**Requires restart:** no.


### enable_topic_message_meta

**Description:** Enables enriched message metadata in topics, including timestamps, message IDs, and custom headers, providing better observability and message tracking capabilities.

**Default:** `true`.

**Requires restart:** no.


### enable_ic_node_cache

**Description:** Enables caching in the Interconnect subsystem for improved node discovery and communication performance, reducing network overhead for cluster internal communication.

**Default:** `true`.

**Requires restart:** yes.


### enable_temp_tables

**Description:** Enables support for temporary tables that exist only for the duration of a session or transaction, useful for intermediate calculations and data processing.

**Default:** `true`.

**Requires restart:** no.


### suppress_compatibility_check

**Description:** Suppresses version compatibility checks during cluster operations, allowing mixed-version deployments. Use with caution as this may lead to compatibility issues.

**Default:** `false`.

**Requires restart:** yes.


### enable_uniq_constraint

**Description:** Enables support for unique constraints on table columns, ensuring data integrity by preventing duplicate values in specified column combinations.

**Default:** `true`.

**Requires restart:** no.


### enable_changefeed_debezium_json_format

**Description:** Enables Debezium-compatible JSON format for changefeeds, facilitating integration with Debezium-based change data capture pipelines and Kafka Connect ecosystems.

**Default:** `true`.

**Requires restart:** no.


### enable_statistics

**Description:** Enables collection of query execution statistics and table statistics for query optimization. This helps the query optimizer make better execution plans.

**Default:** `true`.

**Requires restart:** no.


### enable_uuid_as_primary_key

**Description:** Allows using UUID (Universally Unique Identifier) columns as primary keys, enabling globally unique identifiers for distributed applications.

**Default:** `true`.

**Requires restart:** no.


### enable_table_pg_types

**Description:** Enables support for PostgreSQL-compatible data types in table schemas, facilitating migration from PostgreSQL and providing familiar type system for PostgreSQL developers. For data type reference, see [{#T}](../../yql/reference/types/index.md).

**Default:** `false`.

**Requires restart:** no.


### enable_localdb_btree_index

**Description:** Enables B-tree indexing in LocalDB for improved range query performance and efficient data retrieval patterns requiring ordered access.

**Default:** `true`.

**Requires restart:** no.


### enable_pdisk_high_hdd_in_flight

**Description:** Allows higher number of concurrent in-flight operations specifically for HDD-based PDisks, optimizing throughput for rotating storage at the cost of potentially increased latency variance.

**Default:** `false`.

**Requires restart:** no.


### enable_views

**Description:** Enables support for SQL views, allowing creation of virtual tables based on query results. See [CREATE VIEW syntax](../../yql/reference/syntax/create-view.md) for details.

**Default:** `true`.

**Requires restart:** no.


### enable_serverless_exclusive_dynamic_nodes

**Description:** Enables exclusive allocation of dynamic nodes for Serverless databases, providing better resource isolation and predictable performance in multi-tenant Serverless environments.

**Default:** `false`.

**Requires restart:** no.


### enable_access_service_bulk_authorization

**Description:** Enables bulk authorization processing in the Access Service, allowing efficient batch processing of authorization requests to improve performance in high-throughput scenarios.

**Default:** `false`.

**Requires restart:** no.


### enable_add_colums_with_defaults

**Description:** Enables adding new columns with default values to existing tables, allowing schema evolution without requiring explicit values for existing rows during DDL operations.

**Default:** `false`.

**Requires restart:** no.


### enable_replace_if_exists_for_external_entities

**Description:** Enables `REPLACE IF EXISTS` syntax for external entities like external data sources and connections, providing idempotent creation operations.

**Default:** `false`.

**Requires restart:** no.


### enable_cms_request_priorities

**Description:** Enables request prioritization in the [Cluster Management System (CMS)](../../concepts/glossary.md#cms), allowing high-priority operations to be processed ahead of lower-priority ones for better system responsiveness.

**Default:** `true`.

**Requires restart:** no.


### enable_keyvalue_log_batching

**Description:** Enables batching of KeyValue operations in transaction logs, improving write throughput by combining multiple operations into fewer log entries at the cost of slightly increased latency.

**Default:** `false`.

**Requires restart:** no.


### enable_localdb_flat_index

**Description:** Enables flat indexing in LocalDB for efficient key-based lookups, providing optimal performance for exact-match queries and simple range operations.

**Default:** `true`.

**Requires restart:** no.


### extended_vdisk_counters

**Description:** Enables extended performance counters for VDisk monitoring, providing detailed metrics about virtual disk operations, latency, and resource utilization for enhanced observability.

**Default:** `true`.

**Requires restart:** yes.


### extended_pdisk_sensors

**Description:** Enables extended sensor metrics for PDisk monitoring, providing comprehensive insights into physical disk performance, I/O patterns, and health status for better system diagnostics.

**Default:** `true`.

**Requires restart:** yes.


### enable_stable_node_names

**Description:** Maintains stable node names across restarts and reconfigurations, ensuring consistent node identification for monitoring and administrative tools that rely on persistent node identifiers.

**Default:** `false`.

**Requires restart:** no.


### enable_backup_service

**Description:** Enables the built-in backup service for automated database backups, providing scheduled backup creation, retention policies, and backup management capabilities.

**Default:** `false`.

**Requires restart:** no.


### enable_volatile_transaction_arbiters

**Description:** Enables volatile transaction arbiters for improved transaction coordination performance, using in-memory arbiters that provide faster conflict resolution for short-lived transactions.

**Default:** `true`.

**Requires restart:** no.


### enable_graph_shard

**Description:** Enables Graph shard functionality for graph database capabilities, allowing storage and querying of graph data structures with vertices and edges within {{ ydb-short-name }}.

**Default:** `false`.

**Requires restart:** no.


### enable_external_source_schema_inference

**Description:** Enables automatic schema inference for external data sources, allowing {{ ydb-short-name }} to automatically detect and adapt to the structure of external datasets without manual schema definition.

**Default:** `false`.

**Requires restart:** no.


### enable_db_metadata_cache

**Description:** Enables caching of database metadata to reduce metadata lookup overhead and improve query planning performance, especially beneficial for applications with frequent schema operations.

**Default:** `false`.

**Requires restart:** no.


### enable_table_datetime64

**Description:** Enables support for 64-bit datetime types in table schemas, providing extended date range and higher precision for temporal data compared to standard 32-bit datetime types.

**Default:** `true`.

**Requires restart:** no.


### enable_resource_pools

**Description:** Enables resource pools for workload isolation and resource management, allowing allocation of CPU, memory, and I/O resources to different query workloads for better performance predictability.

**Default:** `true`.

**Requires restart:** no.


### enable_column_statistics

**Description:** Enables collection of detailed per-column statistics including histograms, min/max values, and cardinality information to improve query optimizer decisions and execution plans.

**Default:** `false`.

**Requires restart:** no.


### enable_single_composite_action_group

**Description:** Enables single composite action group optimization for reducing coordination overhead in distributed transactions by grouping multiple actions into a single coordination unit.

**Default:** `false`.

**Requires restart:** no.


### enable_resource_pools_on_serverless

**Description:** Enables resource pool functionality specifically for Serverless database deployments, allowing workload isolation within the serverless execution model.

**Default:** `false`.

**Requires restart:** no.


### enable_vector_index

**Description:** Enables [vector indexes](../../dev/vector-indexes.md) for approximate vector similarity search.

**Default:** `true`.

**Requires restart:** no.


### enable_changefeeds_on_index_tables

**Description:** Allows creating changefeeds on secondary index tables to track changes to indexed data. This enables real-time monitoring of changes to specific data subsets.

**Default:** `true`.

**Requires restart:** no.


### enable_resource_pools_counters

**Description:** Enables detailed performance counters and metrics for resource pools, providing insights into resource utilization, queue depths, and pool efficiency for monitoring and optimization.

**Default:** `false`.

**Requires restart:** no.


### enable_optional_columns_in_column_shard

**Description:** Allows creation of optional (nullable) columns in ColumnShard tables, enabling more flexible schema design for analytical workloads where not all columns may have values.

**Default:** `false`.

**Requires restart:** no.


### enable_granular_timecast

**Description:** Enables granular timecast functionality for fine-grained temporal operations and improved time-based query performance with more precise timestamp handling.

**Default:** `true`.

**Requires restart:** no.


### enable_alter_sharding_in_column_shard

**Description:** Allows runtime modification of sharding configuration in column-oriented tables, enabling dynamic redistribution of data for better load balancing and performance optimization.

**Default:** `false`.

**Requires restart:** no.


### enable_pg_syntax

**Description:** Enables PostgreSQL-compatible SQL syntax and semantics, facilitating migration from PostgreSQL by supporting familiar SQL patterns and functions. For PostgreSQL-compatible SQL syntax reference, see [{#T}](../../postgresql/intro.md).

**Default:** `true`.

**Requires restart:** no.


### enable_tiering_in_column_shard

**Description:** Enables tiering policies in ColumnShard for automatic data lifecycle management, moving older or less frequently accessed data to cheaper storage tiers.

**Default:** `false`.

**Requires restart:** no.


### enable_metadata_objects_on_serverless

**Description:** Enables support for metadata objects like views, functions, and procedures on Serverless database deployments, extending serverless capabilities to include complex database objects.

**Default:** `true`.

**Requires restart:** no.


### enable_olap_compression

**Description:** Enables data compression in OLAP column storage to reduce storage footprint and improve I/O performance for analytical workloads, with trade-offs in CPU usage for compression/decompression.

**Default:** `false`.

**Requires restart:** no.


### enable_external_data_sources_on_serverless

**Description:** Enables external data source connectivity for Serverless databases, allowing federated queries and data integration from external systems within the serverless execution model.

**Default:** `true`.

**Requires restart:** no.


### enable_sparsed_columns

**Description:** Enables sparse column optimization for columns with many null values, improving storage efficiency by only storing non-null values and their positions.

**Default:** `false`.

**Requires restart:** no.


### enable_parameterized_decimal

**Description:** Enables parameterized decimal types with configurable precision and scale, providing flexible numeric data types for financial and scientific applications requiring exact decimal arithmetic.

**Default:** `true`.

**Requires restart:** no.


### enable_immediate_writing_on_bulk_upsert

**Description:** Deprecated.

**Default:** `true`.

**Requires restart:** no.


### enable_insert_write_id_special_column_compatibility

**Description:** Enables compatibility mode for the special `write_id` column during insert operations, maintaining backward compatibility with legacy applications using write ID tracking.

**Default:** `false`.

**Requires restart:** no.


### enable_topic_autopartitioning_for_cdc

**Description:** Enables [auto-partitioning of topics](../../concepts/cdc.md#topic-partitions) for CDC streams on row-oriented tables.

**Default:** `true`.

**Requires restart:** no.


### enable_write_portions_on_insert

**Description:** Deprecated.

**Default:** `true`.

**Requires restart:** no.


### enable_follower_stats

**Description:** Enables system views with information about [history of overloaded partitions](../../dev/system-views.md#top-overload-partitions).

**Default:** `true`.

**Requires restart:** no.


### enable_topic_autopartitioning_for_replication

**Description:** Enables automatic topic partitioning for replication scenarios, optimizing data distribution and throughput for cross-region or cross-cluster data replication workflows.

**Default:** `true`.

**Requires restart:** no.


### enable_drive_serials_discovery

**Description:** Enables automatic discovery and tracking of physical drive serial numbers for enhanced hardware monitoring, inventory management, and failure correlation analysis.

**Default:** `false`.

**Requires restart:** no.


### enable_separate_disk_space_quotas

**Description:** Enables separate disk space quota management for different database components, providing fine-grained resource control and preventing any single component from consuming all available storage.

**Default:** `false`.

**Requires restart:** no.


### enable_antlr4_parser

**Description:** Enables the [ANTLR4](https://github.com/antlr/antlr4)-based SQL parser for improved SQL syntax support, better error reporting, and enhanced compatibility with standard SQL constructs. For YQL syntax details, see [{#T}](../../yql/reference/syntax/index.md).

**Default:** `true`.

**Requires restart:** no.


### enable_release_node_name_on_graceful_shutdown

**Description:** Releases node name reservation during graceful shutdown, allowing the name to be reused by other nodes and improving cluster flexibility during rolling updates.

**Default:** `false`.

**Requires restart:** no.


### force_distconf_disable

**Description:** Forces disabling of [DistConf (Distributed Configuration)](../../contributor/configuration-v2.md) system, falling back to static configuration. This may be needed for specific deployment scenarios or troubleshooting.

**Default:** `false`.

**Requires restart:** no.


### enable_scale_recommender

**Description:** Enables the automatic scale recommendations engine that analyzes workload patterns and suggests optimal scaling configurations for improved performance and cost efficiency.

**Default:** `false`.

**Requires restart:** no.


### enable_vdisk_throttling

**Description:** Enables VDisk I/O throttling to prevent overwhelming storage devices and ensure fair resource allocation across multiple virtual disks sharing the same physical storage.

**Default:** `false`.

**Requires restart:** no.


### enable_datashard_in_memory_state_migration

**Description:** Enables in-memory state migration for DataShard, allowing efficient tablet migration without persisting intermediate state to disk, improving migration speed and reducing I/O overhead.

**Default:** `true`.

**Requires restart:** no.


### enable_datashard_in_memory_state_migration_across_generations

**Description:** Enables in-memory state migration across different DataShard generations, maintaining migration efficiency even when crossing generation boundaries during cluster upgrades.

**Default:** `true`.

**Requires restart:** no.


### disable_localdb_erase_cache

**Description:** Disables LocalDB erase cache optimization, which may be needed for debugging or when erase cache behavior causes issues with specific workloads.

**Default:** `false`.

**Requires restart:** no.


### enable_checksums_export

**Description:** Exports data checksums along with the actual data during export operations, enabling data integrity verification after import or transfer.

**Default:** `false`.

**Requires restart:** no.


### enable_topic_transfer

**Description:** Enables topic transfer operations for moving topics between different clusters or storage locations, supporting data migration and load balancing scenarios.

**Default:** `true`.

**Requires restart:** no.


### enable_view_export

**Description:** Includes database views in backup and export operations, ensuring that view definitions are preserved during data migration and backup procedures.

**Default:** `false`.

**Requires restart:** no.


### enable_column_store

**Description:** Enables column store features including OLAP [column-oriented tables](../../concepts/datamodel/table.md#column-oriented-tables) for analytical workloads, providing columnar storage optimizations for data warehouse and analytics use cases.

**Default:** `false`.

**Requires restart:** no.


### enable_strict_acl_check

**Description:** Enables strict ACL validation that prevents granting permissions to non-existent users and automatically removes permissions when users are deleted, improving security and preventing permission leaks. For access control concepts, see [{#T}](../../security/authorization.md).

**Default:** `false`.

**Requires restart:** no.


### database_yaml_config_allowed

**Description:** Allows YAML-based configuration management at the database level, enabling more flexible and readable configuration.

**Default:** `false`.

**Requires restart:** no.


### enable_strict_user_management

**Description:** Restricts local user and group management operations to cluster or database administrators only, enforcing centralized user management and improving security controls. For user management details, see [{#T}](../../security/authentication.md).

**Default:** `false`.

**Requires restart:** no.


### enable_database_admin

**Description:** Enables the database administrator role with elevated privileges for database-level operations, providing granular administrative control separate from cluster administration.

**Default:** `false`.

**Requires restart:** no.


### enable_changefeeds_import

**Description:** Restores changefeed configurations and state during database import and restore operations, ensuring complete data pipeline recovery.

**Default:** `false`.

**Requires restart:** no.


### enable_permissions_export

**Description:** Includes database object permissions and ACLs in export operations, ensuring security configurations are preserved during data migration and backup procedures.

**Default:** `false`.

**Requires restart:** no.


### enable_data_erasure

**Description:** Enables background data erasure processes for secure deletion of sensitive data, ensuring compliance with data protection regulations and privacy requirements.

**Default:** `false`.

**Requires restart:** no.


### enable_show_create

**Description:** Enables `SHOW CREATE` SQL statements for displaying DDL definitions of database objects like tables, indexes, and views, facilitating schema inspection and documentation. For syntax details, see [{#T}](../../yql/reference/syntax/show_create.md).

**Default:** `false`.

**Requires restart:** no.


### enable_changefeeds_export

**Description:** Includes changefeed configurations in backup and export operations, ensuring complete data pipeline definitions are preserved during migration.

**Default:** `false`.

**Requires restart:** no.


### enable_kafka_native_balancing

**Description:** Enables client-side partition balancing for Kafka protocol consumers, allowing automatic load distribution across consumer instances for optimal throughput.

**Default:** `true`.

**Requires restart:** no.


### enable_tablet_restart_on_unhandled_exceptions

**Description:** Automatically restarts tablets when unhandled exceptions occur, improving system resilience by recovering from unexpected failures and maintaining service availability.

**Default:** `true`.

**Requires restart:** no.


### enable_kafka_transactions

**Description:** Enables transactional operations via the Kafka protocol, providing exactly-once semantics and atomic message processing for Kafka-compatible clients.

**Default:** `true`.

**Requires restart:** no.


### enable_login_cache

**Description:** Enables authentication result caching to reduce login latency and improve user experience by avoiding repeated authentication overhead for frequent operations. For authentication details, see [{#T}](../../security/authentication.md).

**Default:** `false`.

**Requires restart:** no.


### switch_to_config_v2

**Description:** Switches to [configuration version 2](../../devops/configuration-management/configuration-v2/index.md) format, enabling access to newer configuration features and improved configuration management capabilities.

**Default:** `false`.

**Requires restart:** no.


### switch_to_config_v1

**Description:** Switches to [configuration version 1](../../devops/configuration-management/configuration-v1/index.md) format, allowing rollback to older configuration schema when needed for compatibility or troubleshooting purposes.

**Default:** `false`.

**Requires restart:** no.


### enable_encrypted_export

**Description:** Enables encryption support in export operations, protecting exported data with encryption during transfer and storage for enhanced security and compliance.

**Default:** `false`.

**Requires restart:** no.


### enable_alter_database

**Description:** Enables `ALTER DATABASE` DDL operations, allowing runtime modification of database-level settings and properties without requiring cluster restart.

**Default:** `false`.

**Requires restart:** no.


### enable_export_auto_dropping

**Description:** Automatically cleans up temporary tables and directories created during export operations to S3, reducing storage costs by removing intermediate export artifacts.

**Default:** `false`.

**Requires restart:** no.


### enable_throttling_report

**Description:** Enables detailed throttling reports for monitoring and analyzing request throttling patterns, helping identify performance bottlenecks and resource constraints.

**Default:** `true`.

**Requires restart:** no.


### enable_node_broker_delta_protocol

**Description:** Enables delta protocol in NodeBroker for efficient incremental updates to node configuration, reducing network overhead and improving cluster coordination performance.

**Default:** `false`.

**Requires restart:** no.


### enable_access_to_index_impl_tables

**Description:** Allows using followers (read replicas) for covered secondary index queries, improving read performance by distributing index lookup load across multiple replicas.

**Default:** `true`.

**Requires restart:** no.


### enable_add_unique_index

**Description:** Enables creation of unique secondary indexes on existing tables, providing additional uniqueness constraints and improving query performance for unique value lookups.

**Default:** `false`.

**Requires restart:** no.


### enable_shared_metadata_accessor_cache

**Description:** Deprecated.

**Default:** `true`.

**Requires restart:** no.


### require_db_prefix_in_secret_name

**Description:** Requires database prefix in secret names for better namespace isolation and security, preventing accidental access to secrets from different databases.

**Default:** `false`.

**Requires restart:** no.


### enable_system_names_protection

**Description:** Protects system-reserved names from being used by user-created objects, preventing naming conflicts and ensuring system integrity by reserving critical namespaces.

**Default:** `false`.

**Requires restart:** no.


### enable_real_system_view_paths

**Description:** Uses real filesystem paths for system views instead of virtual paths, providing direct access to system metadata through standard path resolution mechanisms.

**Default:** `false`.

**Requires restart:** yes.


### enable_cs_schemas_collapsing

**Description:** Enables ColumnShard schema collapsing optimization that reduces metadata overhead by merging compatible schema versions, improving storage efficiency and query performance.

**Default:** `true`.

**Requires restart:** no.


### enable_move_column_table

**Description:** Enables moving column tables between different storage locations or configurations without data recreation, supporting flexible data lifecycle management.

**Default:** `false`.

**Requires restart:** no.


### enable_arrow_result_set_format

**Description:** Enables Apache Arrow format for query result sets, providing efficient columnar data transfer and better integration with analytical tools and frameworks.

**Default:** `false`.

**Requires restart:** no.


### enable_cs_overloads_subscription_retries

**Description:** Enables retry mechanisms for ColumnShard overload subscriptions, improving resilience when monitoring column storage overload conditions by automatically retrying failed subscription attempts.

**Default:** `false`.

**Requires restart:** no.


### enable_topic_compactification_by_key

**Description:** Enables topic compaction based on message keys, retaining only the latest message for each key to reduce storage usage while maintaining the most recent state for each key.

**Default:** `false`.

**Requires restart:** no.


## Configuration Example

To enable specific features in your {{ ydb-short-name }} deployment, add the desired feature flags to your configuration file. The following example shows how to enable vector index support and automatic topic partitioning for Change Data Capture (CDC):

```yaml
feature_flags:
  enable_vector_index: true
  enable_topic_autopartitioning_for_cdc: true
```
