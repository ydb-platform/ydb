LIBRARY()

SRCS(
    bulk_upsert_retry_state.cpp
    retry.cpp
)

PEERDIR(
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/impl/internal/grpc_connections
    ydb/public/sdk/cpp/src/client/impl/observability
    ydb/public/sdk/cpp/src/client/value
)

END()
