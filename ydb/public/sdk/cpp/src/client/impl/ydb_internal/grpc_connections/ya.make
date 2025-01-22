LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    actions.cpp
    grpc_connections.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/impl/ydb_internal/db_driver_state
    ydb/public/sdk/cpp/src/client/impl/ydb_internal/plain_status
    ydb/public/sdk/cpp/src/client/impl/ydb_internal/thread_pool
    ydb/public/sdk/cpp/src/client/impl/ydb_stats
    ydb/public/sdk/cpp/src/client/resources
    ydb/public/sdk/cpp/src/client/types/exceptions
)

END()
