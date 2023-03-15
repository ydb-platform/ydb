LIBRARY()

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/client/ydb_topic/topic.h)

SRCS(
    topic.h
    proto_accessor.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_topic/codecs

    library/cpp/retry
    ydb/public/sdk/cpp/client/ydb_topic/impl
    ydb/public/sdk/cpp/client/ydb_proto
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/api/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
