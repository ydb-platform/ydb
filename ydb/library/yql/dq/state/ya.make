LIBRARY()

PEERDIR(
    yql/essentials/public/issue
    ydb/library/yql/core/issue/protos
    ydb/library/yql/dq/proto
    ydb/library/yql/providers/pq/proto
    ydb/library/yql/providers/pq/task_meta
)

SRCS(
    dq_state_load_plan.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
