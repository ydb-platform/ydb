LIBRARY()

SRCS(
    local_topic_client_factory.cpp
    local_topic_client.cpp
)

PEERDIR(
    ydb/core/grpc_services
    ydb/core/grpc_services/local_rpc
    ydb/library/yql/providers/pq/gateway/abstract
    ydb/library/yql/providers/pq/gateway/clients/local
    ydb/library/yverify_stream
    ydb/public/sdk/cpp/src/client/topic
)

END()
