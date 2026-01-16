LIBRARY()

SRCS(
    yql_pq_federated_topic_client.cpp
    yql_pq_topic_client.cpp
)

PEERDIR(
    ydb/library/yql/providers/pq/gateway/abstract
    ydb/public/sdk/cpp/src/client/topic
)

END()
