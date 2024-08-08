LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    persqueue.h
)

PEERDIR(
    ydb/public/sdk/cpp_v2/src/client/persqueue_public/impl
    ydb/public/sdk/cpp_v2/src/client/persqueue_public/include

    ydb/public/sdk/cpp_v2/src/client/topic/common
    ydb/public/sdk/cpp_v2/src/client/topic/impl
    ydb/public/sdk/cpp_v2/include/ydb-cpp-sdk/client/topic

    ydb/public/sdk/cpp_v2/src/client/proto
    ydb/public/sdk/cpp_v2/src/client/driver
    ydb/public/sdk/cpp_v2/src/library/retry
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/api/protos
)

END()
