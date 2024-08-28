LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

INCLUDE(../RpcProxyProtocolVersion.txt)

PROTO_NAMESPACE(yt)

SRCS(
    api/config.cpp
    api/client.cpp
    api/client_common.cpp
    api/client_cache.cpp
    api/delegating_client.cpp
    api/delegating_transaction.cpp
    api/distributed_table_sessions.cpp
    api/etc_client.cpp
    api/journal_client.cpp
    api/operation_client.cpp
    api/security_client.cpp
    api/table_client.cpp
    api/query_tracker_client.cpp
    api/helpers.cpp
    api/internal_client.cpp
    api/public.cpp
    api/rowset.cpp
    api/skynet.cpp
    api/transaction.cpp
    api/queue_transaction_mixin.cpp
    api/dynamic_table_transaction_mixin.cpp
    api/persistent_queue.cpp
    api/sticky_transaction_pool.cpp
    api/options.cpp

    api/rpc_proxy/address_helpers.cpp
    api/rpc_proxy/public.cpp
    api/rpc_proxy/config.cpp
    api/rpc_proxy/helpers.cpp
    api/rpc_proxy/client_impl.cpp
    api/rpc_proxy/client_base.cpp
    api/rpc_proxy/connection.cpp
    api/rpc_proxy/connection_impl.cpp
    api/rpc_proxy/file_reader.cpp
    api/rpc_proxy/file_writer.cpp
    api/rpc_proxy/journal_reader.cpp
    api/rpc_proxy/journal_writer.cpp
    api/rpc_proxy/table_mount_cache.cpp
    api/rpc_proxy/table_reader.cpp
    api/rpc_proxy/table_writer.cpp
    api/rpc_proxy/timestamp_provider.cpp
    api/rpc_proxy/transaction.cpp
    api/rpc_proxy/transaction_impl.cpp
    api/rpc_proxy/row_stream.cpp
    api/rpc_proxy/wire_row_stream.cpp

    bundle_controller_client/bundle_controller_client.cpp
    bundle_controller_client/bundle_controller_settings.cpp

    election/public.cpp

    hive/timestamp_map.cpp

    hydra/version.cpp

    chaos_client/config.cpp
    chaos_client/helpers.cpp
    chaos_client/replication_card.cpp
    chaos_client/replication_card_cache.cpp
    chaos_client/replication_card_serialization.cpp

    chunk_client/chunk_replica.cpp
    chunk_client/config.cpp
    chunk_client/data_statistics.cpp
    chunk_client/helpers.cpp
    chunk_client/public.cpp
    chunk_client/read_limit.cpp
    chunk_client/ready_event_reader_base.cpp

    file_client/config.cpp

    journal_client/public.cpp
    journal_client/config.cpp

    cypress_client/public.cpp

    node_tracker_client/node_directory.cpp
    node_tracker_client/helpers.cpp
    node_tracker_client/public.cpp

    object_client/public.cpp
    object_client/helpers.cpp

    scheduler/operation_id_or_alias.cpp
    scheduler/operation_cache.cpp

    security_client/acl.cpp
    security_client/access_control.cpp
    security_client/public.cpp
    security_client/helpers.cpp

    table_client/public.cpp
    table_client/adapters.cpp
    table_client/table_output.cpp
    table_client/blob_reader.cpp
    table_client/check_schema_compatibility.cpp
    table_client/chunk_stripe_statistics.cpp
    table_client/config.cpp
    table_client/column_rename_descriptor.cpp
    table_client/column_sort_schema.cpp
    table_client/comparator.cpp
    table_client/key.cpp
    table_client/key_bound.cpp
    table_client/key_bound_compressor.cpp
    table_client/pipe.cpp
    table_client/versioned_row.cpp
    table_client/unversioned_row.cpp
    table_client/unversioned_value.cpp
    table_client/versioned_reader.cpp
    table_client/row_base.cpp
    table_client/row_batch.cpp
    table_client/row_buffer.cpp
    table_client/schema.cpp
    table_client/schema_serialization_helpers.cpp
    table_client/schemaless_buffered_dynamic_table_writer.cpp
    table_client/schemaless_dynamic_table_writer.cpp
    table_client/serialize.cpp
    table_client/logical_type.cpp
    table_client/merge_table_schemas.cpp
    table_client/name_table.cpp
    table_client/wire_protocol.cpp
    table_client/columnar_statistics.cpp
    table_client/helpers.cpp
    table_client/value_consumer.cpp
    table_client/table_consumer.cpp
    table_client/schemaless_row_reorderer.cpp
    table_client/unordered_schemaful_reader.cpp
    table_client/validate_logical_type.cpp
    table_client/versioned_io_options.cpp
    table_client/composite_compare.cpp
    table_client/columnar.cpp
    table_client/record_codegen_cpp.cpp
    table_client/record_helpers.cpp

    tablet_client/config.cpp
    tablet_client/watermark_runtime_data.cpp
    tablet_client/table_mount_cache_detail.cpp
    tablet_client/table_mount_cache.cpp
    tablet_client/public.cpp
    tablet_client/helpers.cpp

    queue_client/common.cpp
    queue_client/config.cpp
    queue_client/consumer_client.cpp
    queue_client/helpers.cpp
    queue_client/partition_reader.cpp
    queue_client/producer_client.cpp
    queue_client/queue_rowset.cpp

    ypath/rich.cpp
    ypath/parser_detail.cpp

    transaction_client/batching_timestamp_provider.cpp
    transaction_client/config.cpp
    transaction_client/helpers.cpp
    transaction_client/noop_timestamp_provider.cpp
    transaction_client/remote_timestamp_provider.cpp
    transaction_client/timestamp_provider_base.cpp

    misc/config.cpp
    misc/io_tags.cpp
    misc/method_helpers.cpp
    misc/workload.cpp

    job_tracker_client/public.cpp
    job_tracker_client/helpers.cpp

    query_client/query_builder.cpp
    query_client/query_statistics.cpp

    complex_types/check_yson_token.cpp
    complex_types/check_type_compatibility.cpp
    complex_types/infinite_entity.cpp
    complex_types/merge_complex_types.cpp
    complex_types/time_text.cpp
    complex_types/uuid_text.cpp
    complex_types/yson_format_conversion.cpp

    zookeeper/packet.cpp
    zookeeper/protocol.cpp
    zookeeper/requests.cpp

    kafka/packet.cpp
    kafka/protocol.cpp
    kafka/requests.cpp
)

SRCS(
    ${YT_SRCS}
    yt/yt/client/api/rpc_proxy/protocol_version_variables.h.in
)

PEERDIR(
    yt/yt/client/query_tracker_client
    yt/yt/core
    yt/yt/core/http
    yt/yt/core/https
    yt/yt/library/auth
    yt/yt/library/decimal
    yt/yt/library/re2
    yt/yt/library/erasure
    yt/yt/library/numeric
    yt/yt/library/quantile_digest
    yt/yt_proto/yt/client
    library/cpp/json
    contrib/libs/pfr
)

END()

RECURSE(
    arrow
    cache
    driver
    federated
    hedging
    logging
)

RECURSE_FOR_TESTS(
    table_client/unittests
    unittests
)
