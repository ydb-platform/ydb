LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    status.cpp
)

PEERDIR(
    contrib/libs/protobuf
    ydb/public/sdk/cpp/src/client/resources
    ydb/public/sdk/cpp/src/library/grpc/client
    ydb/public/sdk/cpp/src/library/issue
)

END()
