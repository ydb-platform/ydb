LIBRARY()

SRCS(
    private_client.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/public/api/grpc/draft
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/common_client/impl
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/types/status
    ydb/public/sdk/cpp/src/library/issue
)

END()
