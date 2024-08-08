LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    accessor.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/api/protos
    ydb/public/sdk/cpp_v2/src/client/params
    ydb/public/sdk/cpp_v2/src/client/value
    ydb/public/sdk/cpp_v2/src/library/yql/public/issue/protos
)

END()
