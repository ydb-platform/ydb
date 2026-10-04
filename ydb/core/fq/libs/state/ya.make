LIBRARY()

PEERDIR(
    library/cpp/threading/future
    ydb/core/base
    ydb/core/fq/libs/checkpointing/events
    ydb/core/fq/libs/graph_params/proto
    ydb/library/accessor
    ydb/library/actors/core
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/proto
    ydb/library/yql/providers/pq/common
    ydb/library/yql/providers/pq/proto
    ydb/library/yql/providers/pq/task_meta
    ydb/library/yverify_stream
    yql/essentials/core/sql_types
    yql/essentials/minikql
    yql/essentials/public/issue
    yql/essentials/public/issue/protos
)

SRCS(
    dq_stage_state_recovery_info.cpp
    dq_state_load_plan.cpp
    dq_state_load_plan_resolver.cpp
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
