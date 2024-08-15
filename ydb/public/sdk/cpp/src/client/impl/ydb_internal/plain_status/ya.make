LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/headers.inc)

SRCS(
    status.cpp
)

PEERDIR(
    contrib/libs/protobuf
    ydb/public/sdk/cpp/src/library/grpc/client
    ydb/public/sdk/cpp/src/library/yql_common/issue
)

END()
