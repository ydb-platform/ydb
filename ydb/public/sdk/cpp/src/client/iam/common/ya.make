LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    iam.h
)

PEERDIR(
    ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common
    ydb/public/sdk/cpp/src/library/grpc/client
    library/cpp/threading/future
)

END()
