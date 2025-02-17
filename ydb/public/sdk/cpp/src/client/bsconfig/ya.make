LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    storage_config.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/types/operation
    ydb/public/sdk/cpp/src/client/value
)

END()
