LIBRARY(topic)

SRCS(
    topic_read.cpp
    topic_write.cpp
)

PEERDIR(
    ydb/public/lib/ydb_cli/common
    ydb/public/sdk/cpp/client/ydb_proto
    ydb/public/sdk/cpp/client/ydb_persqueue_public
    ydb/public/sdk/cpp/client/ydb_topic
)

GENERATE_ENUM_SERIALIZATION(topic_metadata_fields.h)

END()

RECURSE_FOR_TESTS(
    ut
)
