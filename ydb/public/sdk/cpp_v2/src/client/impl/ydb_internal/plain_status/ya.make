LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    status.cpp
)

PEERDIR(
    contrib/libs/protobuf
    ydb/public/sdk/cpp_v2/src/library/grpc/client
    ydb/public/sdk/cpp_v2/src/library/yql/public/issue
)

END()
