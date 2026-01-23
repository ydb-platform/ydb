LIBRARY()

SRCS(
    local_topic_client_factory.cpp
    local_topic_client.cpp
    local_topic_read_session.cpp
)

PEERDIR(
    library/cpp/protobuf/interop
    ydb/core/base
    ydb/core/grpc_services
    ydb/core/grpc_services/local_rpc
    ydb/core/kqp/common
    ydb/library/actors/core
    ydb/library/yql/providers/pq/gateway/abstract
    ydb/library/yql/providers/pq/gateway/clients/local
    ydb/library/yverify_stream
    ydb/public/sdk/cpp/adapters/issue
    ydb/public/sdk/cpp/src/client/topic
    ydb/services/persqueue_v1/actors
    ydb/services/persqueue_v1
)

END()
