LIBRARY()

SRCS(
    login.cpp
)

PEERDIR(
    ydb/library/login
    ydb/public/api/grpc
    ydb/public/sdk/cpp/client/ydb_types/status
    ydb/public/sdk/cpp/client/impl/ydb_internal/grpc_connections
    ydb/library/yql/public/issue
)

END()
