LIBRARY()

SRCS(
    datashard.h
    datashard__engine_host.h
    datashard__lock_rows.h
    datashard_active_transaction.h
    datashard_cdc_stream_common.h
    datashard_dep_tracker.h
    datashard_direct_transaction.h
    datashard_failpoints.h
    datashard_impl.h
    datashard_kqp.h
    datashard_locks_db.h
    datashard_outreadset.h
    datashard_pipeline.h
    datashard_read_operation.h
    datashard_trans_queue.h
    datashard_txs.h
    datashard_user_db.h
    datashard_user_table.h
    defs.h
    execution_unit.h
    execution_unit_ctors.h
    execution_unit_kind.h
    export_iface.h
    key_conflicts.h
    multi_txids.h
    operation.h
    progress_queue.h
    read_iterator.h
    read_table_scan.h
    setup_sys_locks.h
    build_index/filter_kmeans.cpp
    build_index/fulltext.cpp
    build_index/fulltext_dict.cpp
    build_index/recompute_kmeans.cpp
    build_index/reshuffle_kmeans.cpp
    build_index/secondary_index.cpp
    cdc_stream_heartbeat.cpp
    cdc_stream_scan.cpp
    change_collector_cdc_stream.cpp
    change_record_cdc_serializer.cpp
    change_sender.cpp
    change_sender_async_index.cpp
    change_sender_cdc_stream.cpp
    change_sender_incr_restore.cpp
    change_sender_table_base.cpp
    datashard__init.cpp
    datashard_active_transaction.cpp
    datashard_change_sending.cpp
    datashard_outreadset.cpp
    datashard_s3_uploads.cpp
    datashard_snapshots.cpp
    truncate_unit.cpp
    validate_row_condition.cpp
)

JOIN_SRCS(
    all_alter.cpp
    alter_cdc_stream_unit.cpp
    alter_table_unit.cpp
)

JOIN_SRCS(
    all_backup.cpp
    backup_restore_traits.cpp
    backup_unit.cpp
)

JOIN_SRCS(
    all_build.cpp
    build_and_wait_dependencies_unit.cpp
    build_data_tx_out_rs_unit.cpp
    build_distributed_erase_tx_out_rs_unit.cpp
    build_index/build_index_scan_manager.cpp
    build_scheme_tx_out_rs_unit.cpp
    build_write_out_rs_unit.cpp
)

JOIN_SRCS(
    all_change_collector_m1.cpp
    change_collector.cpp
    change_collector_async_index.cpp
    change_collector_base.cpp
)

JOIN_SRCS(
    all_change_exchange.cpp
    change_exchange.cpp
    change_exchange_split.cpp
)

JOIN_SRCS(
    all_change_record_m1.cpp
    change_record.cpp
    change_record_body_serializer.cpp
)

JOIN_SRCS(
    all_check.cpp
    check_commit_writes_tx_unit.cpp
    check_data_tx_unit.cpp
    check_distributed_erase_tx_unit.cpp
    check_read_unit.cpp
    check_scheme_tx_unit.cpp
    check_snapshot_tx_unit.cpp
    check_write_unit.cpp
)

JOIN_SRCS(
    all_complete.cpp
    complete_data_tx_unit.cpp
    complete_write_unit.cpp
)

JOIN_SRCS(
    all_create.cpp
    create_cdc_stream_unit.cpp
    create_persistent_snapshot_unit.cpp
    create_table_unit.cpp
    create_volatile_snapshot_unit.cpp
)

JOIN_SRCS(
    all_datashard_change_m1.cpp
    datashard_change_receiving.cpp
    datashard_change_sender_activation.cpp
)

JOIN_SRCS(
    all_datashard_cleanup.cpp
    datashard__cleanup_borrowed.cpp
    datashard__cleanup_in_rs.cpp
    datashard__cleanup_tx.cpp
    datashard__cleanup_uncommitted.cpp
)

JOIN_SRCS(
    all_datashard_direct.cpp
    datashard_direct_erase.cpp
    datashard_direct_import.cpp
    datashard_direct_transaction.cpp
    datashard_direct_upload.cpp
)

JOIN_SRCS(
    all_datashard_kqp.cpp
    datashard__kqp_scan.cpp
    datashard_kqp.cpp
)

JOIN_SRCS(
    all_datashard_progress.cpp
    datashard__progress_resend_rs.cpp
    datashard__progress_tx.cpp
)

JOIN_SRCS(
    all_datashard_read.cpp
    datashard__read_columns.cpp
    datashard__read_iterator.cpp
)

JOIN_SRCS(
    all_datashard_repl.cpp
    datashard_repl_apply.cpp
    datashard_repl_offsets.cpp
    datashard_repl_offsets_client.cpp
    datashard_repl_offsets_server.cpp
)

JOIN_SRCS(
    all_datashard_s3_m1.cpp
    datashard__s3_download_txs.cpp
    datashard__s3_upload_txs.cpp
    datashard_s3_download.cpp
    datashard_s3_downloads.cpp
    datashard_s3_upload_rows.cpp
)

JOIN_SRCS(
    all_datashard_schema.cpp
    datashard__schema_changed.cpp
    datashard_schema_snapshots.cpp
)

JOIN_SRCS(
    all_datashard_split.cpp
    datashard_split_dst.cpp
    datashard_split_src.cpp
)

JOIN_SRCS(
    all_datashard_store.cpp
    datashard__store_scan_state.cpp
    datashard__store_table_path.cpp
)

JOIN_SRCS(
    all_datashard_user.cpp
    datashard_user_db.cpp
    datashard_user_table.cpp
)

JOIN_SRCS(
    all_datashard_write.cpp
    datashard__write.cpp
    datashard_write_operation.cpp
)

JOIN_SRCS(
    all_datashard_rest_1_m1.cpp
    datashard.cpp
    datashard__cancel_tx_proposal.cpp
    datashard_cdc_stream_common.cpp
    datashard_common_upload.cpp
    datashard__compact_borrowed.cpp
    datashard__compaction.cpp
    datashard__conditional_erase_rows.cpp
)

JOIN_SRCS(
    all_datashard_rest_2_m1.cpp
    datashard_dep_tracker.cpp
    datashard_distributed_erase.cpp
    datashard__engine_host.cpp
    datashard_failpoints.cpp
    datashard__get_state_tx.cpp
    datashard_loans.cpp
    datashard__lock_rows.cpp
)

JOIN_SRCS(
    all_datashard_rest_3_m1.cpp
    datashard_locks_db.cpp
    datashard__migrate_schemeshard.cpp
    datashard__mon_reset_schema_version.cpp
    datashard__monitoring.cpp
    datashard__object_storage_listing.cpp
    datashard__op_rows.cpp
    datashard_overload.cpp
)

JOIN_SRCS(
    all_datashard_rest_4_m1.cpp
    datashard_pipeline.cpp
    datashard__plan_step.cpp
    datashard__propose_tx_base.cpp
    datashard__readset.cpp
    datashard__snapshot_txs.cpp
    datashard__stats.cpp
    datashard_subdomain_path_id.cpp
)

JOIN_SRCS(
    all_datashard_rest_5.cpp
    datashard_trans_queue.cpp
    datashard__vacuum.cpp
)

JOIN_SRCS(
    all_drop.cpp
    drop_cdc_stream_unit.cpp
    drop_index_notice_unit.cpp
    drop_persistent_snapshot_unit.cpp
    drop_table_unit.cpp
    drop_volatile_snapshot_unit.cpp
)

JOIN_SRCS(
    all_execute.cpp
    execute_commit_writes_tx_unit.cpp
    execute_data_tx_unit.cpp
    execute_distributed_erase_tx_unit.cpp
    execute_write_unit.cpp
)

JOIN_SRCS(
    all_export.cpp
    export_common.cpp
    export_iface.cpp
    export_scan.cpp
)

JOIN_SRCS(
    all_finalize.cpp
    finalize_build_index_unit.cpp
    finalize_plan_tx_unit.cpp
)

JOIN_SRCS(
    all_finish.cpp
    finish_propose_unit.cpp
    finish_propose_write_unit.cpp
)

JOIN_SRCS(
    all_incr.cpp
    incr_restore_helpers.cpp
    incr_restore_scan.cpp
)

JOIN_SRCS(
    all_key.cpp
    key_conflicts.cpp
    key_validator.cpp
)

JOIN_SRCS(
    all_load.cpp
    load_and_wait_in_rs_unit.cpp
    load_in_rs_unit.cpp
    load_tx_details_unit.cpp
    load_write_details_unit.cpp
)

JOIN_SRCS(
    all_make.cpp
    make_scan_snapshot_unit.cpp
    make_snapshot_unit.cpp
)

JOIN_SRCS(
    all_move.cpp
    move_index_unit.cpp
    move_table_unit.cpp
)

JOIN_SRCS(
    all_prepare.cpp
    prepare_data_tx_in_rs_unit.cpp
    prepare_distributed_erase_tx_in_rs_unit.cpp
    prepare_index_validation_unit.cpp
    prepare_scheme_tx_in_rs_unit.cpp
    prepare_write_tx_in_rs_unit.cpp
)

JOIN_SRCS(
    all_read.cpp
    read_op_unit.cpp
    read_table_scan.cpp
    read_table_scan_unit.cpp
)

JOIN_SRCS(
    all_receive.cpp
    receive_snapshot_cleanup_unit.cpp
    receive_snapshot_unit.cpp
)

JOIN_SRCS(
    all_remove.cpp
    remove_lock_change_records.cpp
    remove_locks.cpp
    remove_schema_snapshots.cpp
)

JOIN_SRCS(
    all_store.cpp
    store_and_send_out_rs_unit.cpp
    store_and_send_write_out_rs_unit.cpp
    store_commit_writes_tx_unit.cpp
    store_data_tx_unit.cpp
    store_distributed_erase_tx_unit.cpp
    store_scheme_tx_unit.cpp
    store_snapshot_tx_unit.cpp
    store_write_unit.cpp
)

JOIN_SRCS(
    all_volatile.cpp
    volatile_tx.cpp
    volatile_tx_mon.cpp
)

JOIN_SRCS(
    all_wait.cpp
    wait_for_plan_unit.cpp
    wait_for_stream_clearance_unit.cpp
)

JOIN_SRCS(
    all_misc_1_m1.cpp
    block_fail_point_unit.cpp
    completed_operations_unit.cpp
    conflicts_cache.cpp
    direct_tx_unit.cpp
    erase_rows_condition.cpp
    execution_unit.cpp
    follower_edge.cpp
)

JOIN_SRCS(
    all_misc_2_m1.cpp
    incremental_restore_src_actor.cpp
    initiate_build_index_unit.cpp
    memory_state_migration.cpp
    multi_txids.cpp
    operation.cpp
    plan_queue_unit.cpp
)

JOIN_SRCS(
    all_misc_2_m2.cpp
    build_index/kmeans_helper.cpp
    build_index/local_kmeans.cpp
)

JOIN_SRCS(
    all_misc_3_m1.cpp
    build_index/prefix_kmeans.cpp
    probes.cpp
    protect_scheme_echoes_unit.cpp
    range_ops.cpp
    restore_unit.cpp
    rotate_cdc_stream_unit.cpp
)

JOIN_SRCS(
    all_misc_4_m1.cpp
    build_index/sample_k.cpp
    scan_common.cpp
    stream_scan_common.cpp
    type_serialization.cpp
    build_index/unique_index.cpp
    upload_stats.cpp
)

GENERATE_ENUM_SERIALIZATION(backup_restore_traits.h)
GENERATE_ENUM_SERIALIZATION(change_exchange.h)
GENERATE_ENUM_SERIALIZATION(datashard.h)
GENERATE_ENUM_SERIALIZATION(datashard_active_transaction.h)
GENERATE_ENUM_SERIALIZATION(datashard_s3_upload.h)
GENERATE_ENUM_SERIALIZATION(execution_unit.h)
GENERATE_ENUM_SERIALIZATION(execution_unit_kind.h)
GENERATE_ENUM_SERIALIZATION(operation.h)
GENERATE_ENUM_SERIALIZATION(volatile_tx.h)

RESOURCE(
    ui/index.html datashard/index.html
)

PEERDIR(
    contrib/libs/zstd
    library/cpp/containers/absl
    library/cpp/containers/stack_vector
    library/cpp/digest/md5
    library/cpp/html/pcdata
    library/cpp/json
    library/cpp/json/yson
    library/cpp/lwtrace
    library/cpp/lwtrace/mon
    library/cpp/monlib/service/pages
    library/cpp/string_utils/base64
    library/cpp/string_utils/quote
    ydb/core/actorlib_impl
    ydb/core/backup/common
    ydb/core/base
    ydb/library/json_index
    ydb/core/change_exchange
    ydb/core/engine
    ydb/core/engine/minikql
    ydb/core/formats
    ydb/core/io_formats/ydb_dump
    ydb/core/persqueue/writer
    ydb/core/protos
    ydb/core/scheme
    ydb/core/split
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/tx/long_tx_service/public
    ydb/core/tx/locks
    ydb/core/tx/sequenceproxy/public
    ydb/core/util
    ydb/core/wrappers
    ydb/core/ydb_convert
    ydb/library/aclib
    ydb/library/actors/async
    ydb/library/actors/core
    ydb/library/actors/http
    ydb/library/chunks_limiter
    ydb/library/protobuf_printer
    ydb/library/yql/dq/actors/compute
    yql/essentials/types/binary_json
    yql/essentials/types/dynumber
    yql/essentials/core/minsketch
    yql/essentials/parser/pg_wrapper/interface
    ydb/public/api/protos
    yql/essentials/parser/pg_wrapper/interface
    ydb/services/lib/sharding
    yql/essentials/types/uuid
    ydb/core/io_formats/cell_maker
    ydb/core/io_formats/json
)

YQL_LAST_ABI_VERSION()

IF (OS_WINDOWS)
    CFLAGS(
        -DKIKIMR_DISABLE_S3_OPS
    )
ELSE()
    SRCS(
        export_parquet.cpp
        export_s3_buffer.cpp
        export_s3_uploader.cpp
        export_ydb_dump.cpp
        import_s3.cpp
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    build_index/ut
    ut_bloom_filter
    ut_borrowed_compaction
    ut_change_collector
    ut_change_exchange
    ut_compaction
    ut_disk_quotas
    ut_direct_restore
    ut_erase_rows
    ut_export
    ut_external_blobs
    ut_followers
    ut_incremental_backup
    ut_incremental_restore_scan
    ut_init
    ut_keys
    ut_kqp
    ut_kqp_errors
    ut_kqp_scan
    ut_lock_rows
    ut_locks
    ut_minikql
    ut_minstep
    ut_object_storage_listing
    ut_order
    ut_range_ops
    ut_read_committed
    ut_read_iterator
    ut_read_table
    ut_reassign
    ut_replication
    ut_rs
    ut_sequence
    ut_snapshot
    ut_snapshot_isolation
    ut_stats
    ut_trace
    ut_truncate
    ut_upload_rows
    ut_vacuum
    ut_validate_row_condition
    ut_volatile
    ut_write
)
