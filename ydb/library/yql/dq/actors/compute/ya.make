LIBRARY()

SRCS(
    dq_task_runner_exec_ctx.cpp
    dq_async_compute_actor.cpp
    dq_compute_actor_async_io_factory.cpp
    dq_compute_actor_channels.cpp
    dq_compute_actor_checkpoints.cpp
    dq_compute_actor_metrics.cpp
    dq_compute_actor_stats.cpp
    dq_compute_actor_watermarks.cpp
    dq_compute_actor.cpp
    dq_compute_issues_buffer.cpp
    dq_request_context.h
    dq_request_context.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/wilson/protos
    ydb/library/services
    ydb/library/ydb_issue/proto
    ydb/library/yql/dq/actors/common
    ydb/library/yql/dq/actors/spilling
    ydb/library/yql/dq/common
    ydb/library/yql/dq/proto
    ydb/library/yql/dq/runtime
    ydb/library/yql/dq/tasks
    yql/essentials/minikql
    yql/essentials/minikql/comp_nodes
    yql/essentials/public/issue
    ydb/core/quoter/public
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
