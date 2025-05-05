LIBRARY()

SRCS(
    client.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/impl/ydb_internal/grpc_connections
)

END()
