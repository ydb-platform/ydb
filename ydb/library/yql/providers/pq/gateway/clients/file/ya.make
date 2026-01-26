LIBRARY()

SRCS(
    yql_pq_file_federated_topic_client.cpp
    yql_pq_file_topic_client.cpp
    yql_pq_file_topic_defs.cpp
)

PEERDIR(
    ydb/library/yql/providers/pq/gateway/abstract
)

END()
