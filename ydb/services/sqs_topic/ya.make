LIBRARY()

SRCS(
    sqs_topic_proxy.cpp
    utils.cpp
    send_message.cpp
    statuses.cpp
    error.cpp
)

PEERDIR(
    ydb/library/grpc/server
    ydb/core/base
    ydb/core/client/server
    ydb/core/grpc_services
    ydb/core/mind
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/sdk/cpp/src/library/operation_id
    ydb/public/sdk/cpp/src/client/resources
    ydb/services/datastreams/codes
    ydb/services/lib/actors
    ydb/services/lib/sharding
    ydb/services/persqueue_v1
    ydb/services/sqs_topic/queue_url
    ydb/services/ydb
    ydb/core/persqueue/public/describer
    ydb/core/persqueue/public/mlp
    ydb/core/ymq/attributes
    ydb/core/ymq/base
    ydb/core/ymq/error
)

END()

RECURSE(queue_url)
