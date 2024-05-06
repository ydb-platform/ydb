LIBRARY()

SRCS(
    persqueue.h
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_persqueue_public/impl
    ydb/public/sdk/cpp/client/ydb_persqueue_public/include

    ydb/public/sdk/cpp/client/ydb_topic/common
    ydb/public/sdk/cpp/client/ydb_topic/impl
    ydb/public/sdk/cpp/client/ydb_topic/include

    ydb/public/sdk/cpp/client/ydb_proto
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/api/protos

    library/cpp/retry
)

END()

RECURSE_FOR_TESTS(
    ut
)
