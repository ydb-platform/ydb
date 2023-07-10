LIBRARY()

SRCS(
    dq_async_compute_actor.cpp
    dq_compute_actor_async_io_factory.cpp
    dq_compute_actor_channels.cpp
    dq_compute_actor_checkpoints.cpp
    dq_compute_actor_metrics.cpp
    dq_compute_actor_stats.cpp
    dq_compute_actor_watermarks.cpp
    dq_compute_actor.cpp
    dq_compute_issues_buffer.cpp
    retry_queue.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/wilson/protos
    ydb/library/services
    ydb/library/ydb_issue/proto
    ydb/library/yql/dq/common
    ydb/library/yql/dq/proto
    ydb/library/yql/dq/runtime
    ydb/library/yql/dq/tasks
    ydb/library/yql/minikql/comp_nodes/llvm
    ydb/library/yql/public/issue
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
