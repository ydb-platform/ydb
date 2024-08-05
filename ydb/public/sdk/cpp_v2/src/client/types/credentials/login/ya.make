LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    login.cpp
)

PEERDIR(
    contrib/libs/jwt-cpp
    ydb/public/api/grpc
    ydb/public/sdk/cpp_v2/src/client/types/status
    ydb/public/sdk/cpp_v2/src/client/impl/ydb_internal/grpc_connections
    ydb/public/sdk/cpp_v2/src/library/yql/public/issue
)

END()
