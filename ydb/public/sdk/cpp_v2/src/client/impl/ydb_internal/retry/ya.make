LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    retry.cpp
)

PEERDIR(
    ydb/public/sdk/cpp_v2/src/client/impl/ydb_internal/grpc_connections
)

END()

