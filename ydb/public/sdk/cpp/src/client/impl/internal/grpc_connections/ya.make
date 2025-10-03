LIBRARY()

SRCS(
    actions.cpp
    grpc_connections.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/impl/internal/db_driver_state
    ydb/public/sdk/cpp/src/client/impl/internal/plain_status
    ydb/public/sdk/cpp/src/client/impl/stats
    ydb/public/sdk/cpp/src/client/resources
    ydb/public/sdk/cpp/src/client/types/exceptions
    ydb/public/sdk/cpp/src/client/types/executor
)

END()
