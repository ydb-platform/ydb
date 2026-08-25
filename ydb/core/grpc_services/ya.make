LIBRARY()

ADDINCL(
    ydb/public/sdk/cpp
)

SRCS(
    db_metadata_cache.h
    query/service_query.h
    grpc_mon.cpp
    grpc_publisher_service_actor.cpp
    grpc_request_proxy.cpp
    grpc_request_proxy_simple.cpp
    query/rpc_execute_script.cpp
    rpc_alter_coordination_node.cpp
    rpc_alter_table.cpp
    rpc_begin_transaction.cpp
    rpc_bridge.cpp
    rpc_cluster_state.cpp
    rpc_cms.cpp
    rpc_config.cpp
    rpc_copy_table.cpp
    rpc_copy_tables.cpp
    rpc_create_coordination_node.cpp
    rpc_create_table.cpp
    rpc_describe_coordination_node.cpp
    rpc_describe_external_data_source.cpp
    rpc_describe_external_table.cpp
    rpc_describe_path.cpp
    rpc_describe_secret.cpp
    rpc_describe_system_view.cpp
    rpc_describe_table.cpp
    rpc_describe_table_options.cpp
    rpc_drop_coordination_node.cpp
    rpc_drop_table.cpp
    rpc_execute_scheme_query.cpp
    rpc_execute_yql_script.cpp
    rpc_explain_data_query.cpp
    rpc_explain_yql_script.cpp
    rpc_export.cpp
    rpc_fq.cpp
    rpc_fq_internal.cpp
    rpc_keep_alive.cpp
    rpc_keyvalue.cpp
    rpc_kh_describe.cpp
    rpc_kh_snapshots.cpp
    rpc_list_objects_in_s3_export.cpp
    rpc_list_operations.cpp
    rpc_load_rows.cpp
    rpc_rate_limiter_api.cpp
    rpc_read_columns.cpp
    rpc_read_rows.cpp
    rpc_read_table.cpp
    rpc_remove_directory.cpp
    rpc_rename_tables.cpp
    rpc_replication.cpp
    rpc_rollback_transaction.cpp
    rpc_stream_execute_scan_query.cpp
    rpc_stream_execute_yql_script.cpp
    ydb_over_fq/create_session.cpp
    ydb_over_fq/describe_table.cpp
    ydb_over_fq/execute_data_query.cpp
    ydb_over_fq/explain_data_query.cpp
    ydb_over_fq/keep_alive.cpp
    ydb_over_fq/list_directory.cpp
    # Own TU: this file reaches into ydb/core/client/server, which already
    # PEERDIRs this library. Joining it would force every grpc_services
    # dependent to resolve msgbus symbols.
    legacy/rpc_legacy.cpp
)

JOIN_SRCS(
    all_audit.cpp
    audit_dml_operations.cpp
    audit_log.cpp
    audit_logins.cpp
)

JOIN_SRCS(
    all_execute_m1.cpp
    query/rpc_execute_query.cpp
    rpc_execute_data_query.cpp
)

JOIN_SRCS(
    all_get.cpp
    rpc_get_operation.cpp
    rpc_get_scale_recommendation.cpp
    rpc_get_shard_locations.cpp
)

JOIN_SRCS(
    all_grpc_m1.cpp
    grpc_endpoint_publish_actor.cpp
    grpc_helper.cpp
)

JOIN_SRCS(
    all_import.cpp
    rpc_import.cpp
    rpc_import_data.cpp
)

JOIN_SRCS(
    all_kqp.cpp
    query/rpc_kqp_tx.cpp
    rpc_kqp_base.cpp
)

JOIN_SRCS(
    all_misc_1_m1.cpp
    query/rpc_attach_session.cpp
    rpc_backup.cpp
    rpc_calls.cpp
    rpc_cancel_operation.cpp
)

JOIN_SRCS(
    all_misc_2_m1.cpp
    rpc_commit_transaction.cpp
    rpc_discovery.cpp
)

JOIN_SRCS(
    all_misc_2_m2.cpp
    rpc_common/rpc_common_kqp_session.cpp
    rpc_distributed_storage.cpp
)

JOIN_SRCS(
    all_misc_2_m4.cpp
    rpc_dynamic_config.cpp
    query/rpc_fetch_script_results.cpp
)

JOIN_SRCS(
    all_misc_3_m1.cpp
    rpc_forget_operation.cpp
    fs_path_validation.cpp
    local_rate_limiter.cpp
)

JOIN_SRCS(
    all_misc_3_m4.cpp
    rpc_log_store.cpp
    rpc_login.cpp
)

JOIN_SRCS(
    all_misc_4_m1.cpp
    rpc_maintenance.cpp
    rpc_monitoring.cpp
    rpc_node_registration.cpp
    operation_helpers.cpp
)

JOIN_SRCS(
    all_misc_4_m2.cpp
    rpc_make_directory.cpp
    rpc_object_storage.cpp
)

JOIN_SRCS(
    all_misc_4_m3.cpp
    rpc_modify_permissions.cpp
    rpc_ping.cpp
)

JOIN_SRCS(
    all_misc_5_m1.cpp
    rpc_prepare_data_query.cpp
    resolve_local_db_table.cpp
    rpc_scheme_base.cpp
)

JOIN_SRCS(
    all_misc_6_m1.cpp
    table_settings.cpp
    rpc_test_shard.cpp
    rpc_topic_deferred_publish.cpp
)

JOIN_SRCS(
    all_misc_6_m2.cpp
    rpc_view.cpp
    rpc_whoami.cpp
)

PEERDIR(
    contrib/libs/xxhash
    library/cpp/cgiparam
    library/cpp/digest/old_crc
    ydb/core/actorlib_impl
    ydb/core/audit
    ydb/core/backup/common
    ydb/core/backup/regexp
    ydb/core/base
    ydb/core/control/lib
    ydb/core/counters_info
    ydb/core/discovery
    ydb/core/engine
    ydb/core/formats
    ydb/core/fq/libs/events
    ydb/core/fq/libs/control_plane_proxy/events
    ydb/core/grpc_services/base
    ydb/core/grpc_services/counters
    ydb/core/grpc_services/local_rpc
    ydb/core/grpc_services/cancelation
    ydb/core/health_check
    ydb/core/io_formats/ydb_dump
    ydb/core/kesus/tablet
    ydb/core/kqp/common
    ydb/core/kqp/opt
    ydb/core/local_indexes/bloom
    ydb/core/persqueue/deferred_publish
    ydb/core/protos
    ydb/core/statistics
    ydb/core/scheme
    ydb/core/sys_view
    ydb/core/tx
    ydb/core/tx/datashard
    ydb/core/tx/sharding
    ydb/core/tx/data_events
    ydb/core/tx/schemeshard/olap/bg_tasks/events
    ydb/core/util
    ydb/core/ydb_convert
    ydb/core/security
    ydb/core/security/ldap_auth_provider
    ydb/core/security/sasl
    ydb/library/aclib
    yql/essentials/types/binary_json
    yql/essentials/types/dynumber
    ydb/library/mkql_proto
    ydb/library/persqueue/topic_parser
    ydb/library/protobuf_printer
    ydb/library/yaml_config
    ydb/library/cloud_permissions
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/public/types
    yql/essentials/public/issue
    ydb/library/services
    ydb/public/api/grpc/draft
    ydb/public/api/protos
    ydb/public/lib/fq
    ydb/public/sdk/cpp/src/library/operation_id
    ydb/public/sdk/cpp/src/client/resources
)


DEFAULT(YDB_EMBEDDED_NBS_ENABLED yes)

IF (OS_LINUX AND YDB_EMBEDDED_NBS_ENABLED)
    CFLAGS(
        -DYDB_EMBEDDED_NBS_ENABLED
    )
    SRCS(
        rpc_nbs.cpp
        rpc_nbs_io.cpp
    )
    PEERDIR(
        ydb/core/nbs/cloud/blockstore/libs/service
        ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct
        ydb/core/nbs/cloud/blockstore/libs/storage/ss_proxy
        ydb/core/nbs/cloud/blockstore/public/api/protos
        ydb/core/nbs/cloud/storage/core/libs/common
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    base
    counters
    local_rpc
    tablet
)

RECURSE_FOR_TESTS(
    ut
    grpc_request_check_actor_ut
    grpc_request_tracing_ut
)
