LIBRARY()

OWNER(
    xenoxeno
    g:kikimr
)

SRCS(
    backends.cpp
    backends.h
    log.h
    schema.h
    shard_impl.cpp
    shard_impl.h
    tx_init_schema.cpp
    tx_monitoring.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/base
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/graph/api
    ydb/core/graph/shard/protos
)

END()

RECURSE_FOR_TESTS(ut)
