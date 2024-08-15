LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/headers.inc)

SRCS(
    retry.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/impl/ydb_internal/grpc_connections
)

END()

