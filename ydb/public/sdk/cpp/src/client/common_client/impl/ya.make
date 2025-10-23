LIBRARY()

SRCS(
    client.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/impl/internal/grpc_connections
    ydb/public/sdk/cpp/src/library/time
)

END()
