LIBRARY(topic)

SRCS(
    topic_read.cpp
    topic_write.cpp
)

PEERDIR(
    ydb/public/lib/ydb_cli/common
    ydb/public/sdk/cpp/src/client/proto
    ydb/public/sdk/cpp/src/client/persqueue_public
    ydb/public/sdk/cpp/src/client/topic
)

GENERATE_ENUM_SERIALIZATION(topic_metadata_fields.h)

END()

RECURSE_FOR_TESTS(
    ut
)
