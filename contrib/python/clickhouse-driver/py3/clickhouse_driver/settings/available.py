from .types import (
    SettingUInt64, SettingBool, SettingFloat, SettingString, SettingMaxThreads,
    SettingChar
)

SettingInt64 = SettingUInt64

# Seconds and milliseconds should be set as ints.
SettingSeconds = SettingMilliseconds = SettingUInt64

# Server cares about possible choices validation.
# See https://github.com/yandex/ClickHouse/blob/master/dbms/src/
# Interpreters/Settings.h for all choices.
SettingLoadBalancing = SettingTotalsMode = SettingCompressionMethod = \
    SettingDistributedProductMode = SettingGlobalSubqueriesMethod = \
    SettingDateTimeInputFormat = \
    SettingURI = \
    SettingJoinAlgorithm = \
    SettingSpecialSort = \
    SettingLogQueriesType = \
    SettingDefaultDatabaseEngine = \
    SettingString

settings = {
    # Settings
    'min_compress_block_size': SettingUInt64,
    'max_compress_block_size': SettingUInt64,
    'max_block_size': SettingUInt64,
    'max_insert_block_size': SettingUInt64,
    'min_insert_block_size_rows': SettingUInt64,
    'min_insert_block_size_bytes': SettingUInt64,
    'max_partitions_per_insert_block': SettingUInt64,
    'max_threads': SettingMaxThreads,
    'max_alter_threads': SettingMaxThreads,
    'max_read_buffer_size': SettingUInt64,
    'max_distributed_connections': SettingUInt64,
    'max_query_size': SettingUInt64,
    'interactive_delay': SettingUInt64,
    'connect_timeout': SettingSeconds,
    'connect_timeout_with_failover_ms': SettingMilliseconds,
    'receive_timeout': SettingSeconds,
    'send_timeout': SettingSeconds,
    'queue_max_wait_ms': SettingMilliseconds,
    'poll_interval': SettingUInt64,
    'distributed_connections_pool_size': SettingUInt64,
    'connections_with_failover_max_tries': SettingUInt64,
    'extremes': SettingBool,
    'use_uncompressed_cache': SettingBool,
    'replace_running_query': SettingBool,
    'background_pool_size': SettingUInt64,
    'background_schedule_pool_size': SettingUInt64,

    'distributed_directory_monitor_sleep_time_ms': SettingMilliseconds,

    'distributed_directory_monitor_batch_inserts': SettingBool,
    'distributed_directory_monitor_max_sleep_time_ms': SettingMilliseconds,

    'optimize_move_to_prewhere': SettingBool,
    'optimize_skip_unused_shards': SettingBool,
    'optimize_read_in_order': SettingBool,
    'optimize_min_equality_disjunction_chain_length': SettingUInt64,
    'enable_optimize_predicate_expression': SettingBool,

    'replication_alter_partitions_sync': SettingUInt64,
    'replication_alter_columns_timeout': SettingUInt64,

    'load_balancing': SettingLoadBalancing,

    'totals_mode': SettingTotalsMode,
    'totals_auto_threshold': SettingFloat,

    'compile': SettingBool,
    'compile_expressions': SettingBool,
    'min_count_to_compile': SettingUInt64,
    'group_by_two_level_threshold': SettingUInt64,
    'group_by_two_level_threshold_bytes': SettingUInt64,
    'distributed_aggregation_memory_efficient': SettingBool,
    'aggregation_memory_efficient_merge_threads': SettingUInt64,

    'max_parallel_replicas': SettingUInt64,
    'parallel_replicas_count': SettingUInt64,
    'parallel_replica_offset': SettingUInt64,

    'skip_unavailable_shards': SettingBool,

    'distributed_group_by_no_merge': SettingBool,

    'merge_tree_min_rows_for_concurrent_read': SettingUInt64,
    'merge_tree_min_rows_for_seek': SettingUInt64,
    'merge_tree_coarse_index_granularity': SettingUInt64,
    'merge_tree_max_rows_to_use_cache': SettingUInt64,
    'merge_tree_min_bytes_for_concurrent_read': SettingUInt64,
    'merge_tree_min_bytes_for_seek': SettingUInt64,
    'merge_tree_max_bytes_to_use_cache': SettingUInt64,
    'merge_tree_uniform_read_distribution': SettingBool,

    'mysql_max_rows_to_insert': SettingUInt64,

    'min_bytes_to_use_direct_io': SettingUInt64,

    'force_index_by_date': SettingBool,
    'force_primary_key': SettingBool,

    'mark_cache_min_lifetime': SettingUInt64,

    'max_streams_to_max_threads_ratio': SettingFloat,

    'network_compression_method': SettingCompressionMethod,

    'network_zstd_compression_level': SettingInt64,

    'priority': SettingUInt64,

    'log_queries': SettingBool,
    'log_queries_cut_to_length': SettingUInt64,
    'query_profiler_real_time_period_ns': SettingUInt64,
    'query_profiler_cpu_time_period_ns': SettingUInt64,
    'enable_debug_queries': SettingBool,

    'distributed_product_mode': SettingDistributedProductMode,

    'max_concurrent_queries_for_user': SettingUInt64,

    'insert_deduplicate': SettingBool,

    'insert_quorum': SettingUInt64,
    'insert_quorum_timeout': SettingMilliseconds,
    'select_sequential_consistency': SettingUInt64,
    'table_function_remote_max_addresses': SettingUInt64,
    'read_backoff_min_latency_ms': SettingMilliseconds,
    'read_backoff_max_throughput': SettingUInt64,
    'read_backoff_min_interval_between_events_ms': SettingMilliseconds,
    'read_backoff_min_events': SettingUInt64,

    'memory_tracker_fault_probability': SettingFloat,

    'enable_http_compression': SettingBool,
    'http_zlib_compression_level': SettingInt64,

    'http_native_compression_disable_checksumming_on_decompress': SettingBool,

    'count_distinct_implementation': SettingString,

    'add_http_cors_header': SettingBool,

    'input_format_skip_unknown_fields': SettingBool,
    'input_format_import_nested_json': SettingBool,
    'input_format_values_interpret_expressions': SettingBool,
    'input_format_with_names_use_header': SettingBool,
    'input_format_defaults_for_omitted_fields': SettingBool,
    'input_format_null_as_default': SettingBool,
    'input_format_values_deduce_templates_of_expressions': SettingBool,
    'input_format_values_accurate_types_of_literals': SettingBool,
    'input_format_allow_errors_num': SettingUInt64,
    'input_format_allow_errors_ratio': SettingFloat,
    'input_format_csv_unquoted_null_literal_as_null': SettingBool,

    'output_format_write_statistics': SettingBool,
    'output_format_json_quote_64bit_integers': SettingBool,
    'output_format_json_quote_denormals': SettingBool,
    'output_format_json_escape_forward_slashes': SettingBool,
    'output_format_pretty_max_rows': SettingUInt64,
    'output_format_pretty_max_column_pad_width': SettingUInt64,
    'output_format_pretty_color': SettingBool,
    'output_format_parquet_row_group_size': SettingUInt64,

    'use_client_time_zone': SettingBool,

    'send_progress_in_http_headers': SettingBool,

    'http_headers_progress_interval_ms': SettingUInt64,

    'fsync_metadata': SettingBool,

    'join_use_nulls': SettingBool,
    'join_default_strictness': SettingString,
    'preferred_block_size_bytes': SettingUInt64,

    'max_replica_delay_for_distributed_queries': SettingUInt64,
    'fallback_to_stale_replicas_for_distributed_queries': SettingBool,
    'preferred_max_column_in_block_size_bytes': SettingUInt64,

    'insert_distributed_sync': SettingBool,
    'insert_distributed_timeout': SettingUInt64,
    'distributed_ddl_task_timeout': SettingInt64,
    'stream_flush_interval_ms': SettingMilliseconds,
    'format_schema': SettingString,
    'insert_allow_materialized_columns': SettingBool,
    'http_connection_timeout': SettingSeconds,
    'http_send_timeout': SettingSeconds,
    'http_receive_timeout': SettingSeconds,
    'optimize_throw_if_noop': SettingBool,
    'use_index_for_in_with_subqueries': SettingBool,
    'empty_result_for_aggregation_by_empty_set': SettingBool,
    'allow_distributed_ddl': SettingBool,
    'odbc_max_field_size': SettingUInt64,

    # Limits
    'max_rows_to_read': SettingUInt64,
    'max_bytes_to_read': SettingUInt64,
    'read_overflow_mode': SettingString,

    'max_rows_to_group_by': SettingUInt64,
    'group_by_overflow_mode': SettingString,
    'max_bytes_before_external_group_by': SettingUInt64,

    'max_rows_to_sort': SettingUInt64,
    'max_bytes_to_sort': SettingUInt64,
    'sort_overflow_mode': SettingString,
    'max_bytes_before_external_sort': SettingUInt64,
    'max_bytes_before_remerge_sort': SettingUInt64,
    'max_result_rows': SettingUInt64,
    'max_result_bytes': SettingUInt64,
    'result_overflow_mode': SettingString,

    'max_execution_time': SettingSeconds,
    'timeout_overflow_mode': SettingString,

    'min_execution_speed': SettingUInt64,
    'timeout_before_checking_execution_speed': SettingSeconds,

    'max_columns_to_read': SettingUInt64,
    'max_temporary_columns': SettingUInt64,
    'max_temporary_non_const_columns': SettingUInt64,

    'max_subquery_depth': SettingUInt64,
    'max_pipeline_depth': SettingUInt64,
    'max_ast_depth': SettingUInt64,
    'max_ast_elements': SettingUInt64,
    'max_expanded_ast_elements': SettingUInt64,

    'readonly': SettingUInt64,

    'max_rows_in_set': SettingUInt64,
    'max_bytes_in_set': SettingUInt64,
    'set_overflow_mode': SettingString,

    'max_rows_in_join': SettingUInt64,
    'max_bytes_in_join': SettingUInt64,
    'join_overflow_mode': SettingString,

    'max_rows_to_transfer': SettingUInt64,
    'max_bytes_to_transfer': SettingUInt64,
    'transfer_overflow_mode': SettingString,

    'max_rows_in_distinct': SettingUInt64,
    'max_bytes_in_distinct': SettingUInt64,
    'distinct_overflow_mode': SettingString,

    'max_memory_usage': SettingUInt64,
    'max_memory_usage_for_user': SettingUInt64,
    'max_memory_usage_for_all_queries': SettingUInt64,

    'max_network_bandwidth': SettingUInt64,
    'max_network_bytes': SettingUInt64,
    'max_network_bandwidth_for_user': SettingUInt64,
    'max_network_bandwidth_for_all_users': SettingUInt64,

    'max_streams_multiplier_for_merge_tables': SettingFloat,
    'max_http_get_redirects': SettingUInt64,
    'max_execution_speed': SettingUInt64,
    'max_execution_speed_bytes': SettingUInt64,

    'format_csv_delimiter': SettingChar,
    'format_csv_allow_single_quotes': SettingBool,
    'format_csv_allow_double_quotes': SettingBool,

    'format_template_resultset': SettingString,
    'format_template_row': SettingString,
    'format_template_rows_between_delimiter': SettingString,
    'format_custom_escaping_rule': SettingString,
    'format_custom_field_delimiter': SettingString,
    'format_custom_row_before_delimiter': SettingString,
    'format_custom_row_after_delimiter': SettingString,
    'format_custom_row_between_delimiter': SettingString,
    'format_custom_result_before_delimiter': SettingString,
    'format_custom_result_after_delimiter': SettingString,

    'enable_conditional_computation': SettingUInt64,

    'date_time_input_format': SettingDateTimeInputFormat,
    'log_profile_events': SettingBool,
    'log_query_settings': SettingBool,
    'log_query_threads': SettingBool,
    'send_logs_level': SettingString,
    'low_cardinality_max_dictionary_size': SettingUInt64,
    'low_cardinality_use_single_dictionary_for_part': SettingBool,
    'decimal_check_overflow': SettingBool,
    'prefer_localhost_replica': SettingBool,
    'max_fetch_partition_retries_count': SettingUInt64,
    'asterisk_left_columns_only': SettingBool,
    'http_max_multipart_form_data_size': SettingUInt64,
    'calculate_text_stack_trace': SettingBool,
    'parallel_view_processing': SettingBool,

    'allow_experimental_low_cardinality_type': SettingBool,
    'allow_experimental_decimal_type': SettingBool,
    'allow_suspicious_low_cardinality_types': SettingBool,
    'allow_experimental_multiple_joins_emulation': SettingBool,
    'allow_experimental_cross_to_join_conversion': SettingBool,
    'allow_experimental_data_skipping_indices': SettingBool,
    'allow_hyperscan': SettingBool,
    'allow_simdjson': SettingBool,
    'allow_introspection_functions': SettingBool,
    'allow_drop_detached': SettingBool,
    'allow_experimental_live_view': SettingBool,
    'allow_ddl': SettingBool,

    'partial_merge_join': SettingBool,
    'partial_merge_join_optimizations': SettingBool,
    'partial_merge_join_rows_in_right_blocks': SettingUInt64,
    'partial_merge_join_rows_in_left_blocks': SettingFloat,

    'distributed_replica_error_half_life': SettingSeconds,
    'distributed_replica_error_cap': SettingUInt64,

    'min_free_disk_space_for_temporary_data': SettingUInt64,
    'tcp_keep_alive_timeout': SettingSeconds,
    'connection_pool_max_wait_ms': SettingMilliseconds,
    'kafka_max_wait_ms': SettingMilliseconds,
    'idle_connection_timeout': SettingUInt64,
    's3_min_upload_part_size': SettingUInt64,
    'any_join_distinct_right_table_keys': SettingBool,
    'join_any_take_last_row': SettingBool,
    'stream_poll_timeout_ms': SettingMilliseconds,
    'joined_subquery_requires_alias': SettingBool,
    'enable_unaligned_array_join': SettingBool,
    'low_cardinality_allow_in_native_format': SettingBool,
    'external_table_functions_use_nulls': SettingBool,
    'experimental_use_processors': SettingBool,
    'check_query_single_value_result': SettingBool,
    'live_view_heartbeat_interval': SettingSeconds,
    'temporary_live_view_timeout': SettingSeconds,
    'max_live_view_insert_blocks_before_refresh': SettingUInt64,

    'max_insert_threads': SettingUInt64,
    'replace_running_query_max_wait_ms': SettingMilliseconds,
    'background_move_pool_size': SettingUInt64,
    'min_count_to_compile_expression': SettingUInt64,
    'force_optimize_skip_unused_shards': SettingUInt64,
    'input_format_parallel_parsing': SettingBool,
    'min_chunk_bytes_for_parallel_parsing': SettingUInt64,
    'min_bytes_to_use_mmap_io': SettingUInt64,
    'os_thread_priority': SettingInt64,
    'input_format_tsv_empty_as_default': SettingBool,
    'input_format_avro_schema_registry_url': SettingString,
    'output_format_avro_codec': SettingString,
    'output_format_avro_sync_interval': SettingUInt64,
    'min_execution_speed_bytes': SettingUInt64,
    'default_max_bytes_in_join': SettingUInt64,
    'enable_optimize_predicate_expression_to_final_subquery': SettingBool,
    'cancel_http_readonly_queries_on_client_close': SettingBool,
    'enable_scalar_subquery_optimization': SettingBool,
    'optimize_trivial_count_query': SettingBool,
    'mutations_sync': SettingUInt64,
    'optimize_if_chain_to_miltiif': SettingBool,
    'max_parser_depth': SettingUInt64,

    'max_joined_block_size_rows': SettingUInt64,
    'connect_timeout_with_failover_secure_ms': SettingMilliseconds,
    'parallel_distributed_insert_select': SettingBool,
    'force_optimize_skip_unused_shards_no_nested': SettingBool,
    'format_avro_schema_registry_url': SettingURI,
    'output_format_tsv_crlf_end_of_line': SettingBool,
    'join_algorithm': SettingJoinAlgorithm,
    'memory_profiler_step': SettingUInt64,
    'output_format_csv_crlf_end_of_line': SettingBool,
    'allow_experimental_alter_materialized_view_structure': SettingBool,
    'enable_early_constant_folding': SettingBool,
    'deduplicate_blocks_in_dependent_materialized_views': SettingBool,
    'use_compact_format_in_distributed_parts_names': SettingBool,
    'multiple_joins_rewriter_version': SettingUInt64,

    'min_insert_block_size_rows_for_materialized_views': SettingUInt64,
    'min_insert_block_size_bytes_for_materialized_views': SettingUInt64,
    'max_final_threads': SettingUInt64,
    'background_buffer_flush_schedule_pool_size': SettingUInt64,
    'background_distributed_schedule_pool_size': SettingUInt64,
    'special_sort': SettingSpecialSort,
    'optimize_distributed_group_by_sharding_key': SettingBool,
    'log_queries_min_type': SettingLogQueriesType,
    'allow_suspicious_codecs': SettingBool,
    'metrics_perf_events_enabled': SettingBool,
    'metrics_perf_events_list': SettingString,
    'join_on_disk_max_files_to_merge': SettingUInt64,
    'temporary_files_codec': SettingString,
    'max_untracked_memory': SettingUInt64,
    'memory_profiler_sample_probability': SettingFloat,
    'optimize_aggregation_in_order': SettingBool,
    'default_database_engine': SettingDefaultDatabaseEngine,
    'allow_experimental_database_atomic': SettingBool,
    'show_table_uuid_in_table_create_query_if_not_nil': SettingBool,
    'optimize_arithmetic_operations_in_aggregate_functions': SettingBool,
    'validate_polygons': SettingBool,
    'transform_null_in': SettingBool,
    'allow_nondeterministic_mutations': SettingBool,
    'lock_acquire_timeout': SettingSeconds,
    'materialize_ttl_after_modify': SettingBool,
    'allow_experimental_geo_types': SettingBool,
    'output_format_pretty_max_value_width': SettingUInt64,
    'format_regexp': SettingString,
    'format_regexp_escaping_rule': SettingString,
    'format_regexp_skip_unmatched': SettingBool,
    'output_format_enable_streaming': SettingBool,
}
