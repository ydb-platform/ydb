LIBRARY()

SRCS(
    backends.cpp
    backends.h
    log.h
    schema.h
    shard_impl.cpp
    shard_impl.h
    tx_aggregate_data.cpp
    tx_change_backend.cpp
    tx_get_metrics.cpp
    tx_init_schema.cpp
    tx_monitoring.cpp
    tx_startup.cpp
    tx_store_metrics.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/base
    ydb/core/cms/console
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/graph/api
    ydb/core/graph/shard/protos
)

END()

RECURSE_FOR_TESTS(ut)
