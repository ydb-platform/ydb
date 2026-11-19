YQL_LIBRARY()

SRCS(
    streaming_queries.cpp
)

PEERDIR(
    library/cpp/protobuf/json
    ydb/core/kqp/common/events
    ydb/core/kqp/common/simple
    ydb/core/kqp/gateway/behaviour/streaming_query/common
    ydb/core/kqp/runtime
    ydb/core/sys_view/common
    ydb/library/actors/core
    ydb/library/query_actor
    ydb/services/workload_manager/actors
)

END()
