LIBRARY()

SRCS(
    kqp_execution_tracing.cpp
    kqp_scan_tracing.cpp
    kqp_shard_tracing.cpp
    kqp_query_tracing.cpp
    kqp_task_tracing.cpp
)

PEERDIR(
    library/cpp/time_provider
    ydb/core/kqp/common/simple
    ydb/core/protos
    ydb/library/actors/wilson
    ydb/library/wilson_ids
    ydb/library/yql/dq/proto
    ydb/library/yql/dq/actors
    ydb/library/yql/dq/actors/protos
    ydb/library/security
    ydb/public/api/protos
    yql/essentials/ast
)

END()
