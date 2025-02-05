LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/client/forbid_peerdir.inc)

SRCS(
    retry.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/impl/ydb_internal/grpc_connections
)

END()

