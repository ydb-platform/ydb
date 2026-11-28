LIBRARY()

SRCS(
    yql_kafka_message_stream_client.cpp
    yql_pq_message_stream_client.cpp
)

PEERDIR(
    contrib/libs/librdkafka/src-cpp
    ydb/library/actors/core
    ydb/library/yql/providers/abstract
    ydb/public/api/protos
    ydb/public/sdk/cpp/adapters/issue
    ydb/library/yverify_stream
    ydb/public/sdk/cpp/src/client/topic
    yql/essentials/providers/common/structured_token
)

END()
