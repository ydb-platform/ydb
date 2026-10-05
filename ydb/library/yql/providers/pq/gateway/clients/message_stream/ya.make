LIBRARY()

SRCS(
    yql_pq_message_stream_client.cpp
)

PEERDIR(
    ydb/library/yql/providers/abstract
    ydb/public/api/protos
    ydb/public/sdk/cpp/adapters/issue
    ydb/library/yverify_stream
    ydb/public/sdk/cpp/src/client/topic
)

END()
