LIBRARY()

SRCS(
    background_controller.cpp
    blob.cpp
    blob_cache.cpp
    columnshard__init.cpp
    columnshard__notify_tx_completion.cpp
    columnshard__plan_step.cpp
    columnshard__progress_tx.cpp
    columnshard__propose_cancel.cpp
    columnshard__propose_transaction.cpp
    columnshard__scan.cpp
    columnshard__statistics.cpp
    columnshard__write.cpp
    columnshard__write_index.cpp
    columnshard.cpp
    columnshard_impl.cpp
    columnshard_common.cpp
    columnshard_private_events.cpp
    columnshard_schema.cpp
    columnshard_view.cpp
    counters.cpp
    defs.cpp
    inflight_request_tracker.cpp
    write_actor.cpp
    tables_manager.cpp
)

GENERATE_ENUM_SERIALIZATION(columnshard.h)
GENERATE_ENUM_SERIALIZATION(columnshard_impl.h)

PEERDIR(
    ydb/library/actors/core
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/control
    ydb/core/formats
    ydb/core/kqp
    ydb/core/protos
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/tx/time_cast
    ydb/core/tx/columnshard/engines
    ydb/core/tx/columnshard/engines/writer
    ydb/core/tx/columnshard/engines/reader/abstract
    ydb/core/tx/columnshard/counters
    ydb/core/tx/columnshard/common
    ydb/core/tx/columnshard/splitter
    ydb/core/tx/columnshard/operations
    ydb/core/tx/columnshard/transactions
    ydb/core/tx/columnshard/transactions/operators
    ydb/core/tx/columnshard/blobs_reader
    ydb/core/tx/columnshard/blobs_action
    ydb/core/tx/columnshard/data_locks
    ydb/core/tx/columnshard/data_sharing
    ydb/core/tx/columnshard/subscriber
    ydb/core/tx/columnshard/export
    ydb/core/tx/columnshard/resource_subscriber
    ydb/core/tx/columnshard/normalizer
    ydb/core/tx/columnshard/blobs_action/storages_manager
    ydb/core/tx/tiering
    ydb/core/tx/conveyor/usage
    ydb/core/tx/tracing
    ydb/core/tx/long_tx_service/public
    ydb/core/util
    ydb/public/api/protos
    ydb/library/yql/dq/actors/compute
    ydb/library/chunks_limiter
)

IF (OS_WINDOWS)
    CFLAGS(
        -DKIKIMR_DISABLE_S3_OPS
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    engines
    splitter
)

RECURSE_FOR_TESTS(
    ut_rw
    ut_schema
)
