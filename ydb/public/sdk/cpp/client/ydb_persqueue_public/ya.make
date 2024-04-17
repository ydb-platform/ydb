LIBRARY()

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h)

SRCS(
    persqueue.h
)

PEERDIR(
    library/cpp/retry
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/api/protos
    ydb/public/sdk/cpp/client/ydb_proto
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/sdk/cpp/client/ydb_persqueue_public/impl
    ydb/public/sdk/cpp/client/ydb_topic/codecs
    ydb/public/sdk/cpp/client/ydb_topic/common
    ydb/public/sdk/cpp/client/ydb_topic/impl
)

END()

RECURSE_FOR_TESTS(
    ut
)
