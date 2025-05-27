LIBRARY()

SRCS(
    login.cpp
)

PEERDIR(
    contrib/libs/jwt-cpp
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/types/status
    ydb/public/sdk/cpp/src/client/impl/ydb_internal/grpc_connections
    ydb/public/sdk/cpp/src/library/issue
)

END()
