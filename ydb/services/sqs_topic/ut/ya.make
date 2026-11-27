UNITTEST()

SRCS(
    consumer_attributes_ut.cpp
    utils_ut.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/core/testlib/actors
    ydb/core/testlib/default
    ydb/core/tx/scheme_cache
    ydb/library/actors/core
    ydb/services/sqs_topic
    ydb/services/sqs_topic/queue_url
)

END()
