LIBRARY()

SRCS(
    yql_pq_dummy_gateway.cpp
    yql_pq_file_topic_client.cpp
)

PEERDIR(
    ydb/library/yql/providers/pq/provider
)

END()
