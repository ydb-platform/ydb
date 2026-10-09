LIBRARY()

SRCS(
    physical_graph_rescaling.cpp
    pq_topic_resolver.cpp
    streaming_query_controller.cpp
)

PEERDIR(
    library/cpp/protobuf/interop
    ydb/core/base
    ydb/core/fq/libs/checkpointing
    ydb/core/fq/libs/state
    ydb/core/kqp/common
    ydb/core/kqp/federated_query/actors
    ydb/core/protos
    ydb/library/actors/core
    ydb/library/services
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/proto
    ydb/library/yql/dq/tasks
    ydb/library/yql/providers/pq/common
    ydb/library/yql/providers/pq/gateway/abstract
    ydb/library/yql/providers/pq/proto
    ydb/library/yverify_stream
    yql/essentials/core/issue
    yql/essentials/providers/common/proto
    yql/essentials/providers/common/structured_token
    yql/essentials/public/issue
)

YQL_LAST_ABI_VERSION()

END()
