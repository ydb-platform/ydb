LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    client.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/common_client/impl
)

END()
