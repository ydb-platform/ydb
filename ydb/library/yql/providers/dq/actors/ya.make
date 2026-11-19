YQL_LIBRARY()

SRCS(
    compute_actor.cpp
    events.cpp
    executer_actor.cpp
    execution_helpers.cpp
    grouped_issues.cpp
    proto_builder.cpp
    resource_allocator.cpp
    result_receiver.cpp
    task_controller.cpp
    worker_actor.cpp
)

PEERDIR(
    library/cpp/yson
    ydb/core/fq/libs/checkpointing/events
    ydb/library/actors/core
    ydb/library/mkql_proto
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/actors/task_runner
    ydb/library/yql/dq/common
    ydb/library/yql/dq/proto
    ydb/library/yql/dq/runtime
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/dq/common
    ydb/library/yql/providers/dq/counters
    ydb/library/yql/providers/dq/planner
    ydb/library/yql/providers/dq/runtime
    ydb/library/yql/providers/dq/worker_manager/interface
    ydb/library/yql/utils/actor_log
    ydb/library/yql/utils/actors
    ydb/public/api/protos
    yql/essentials/core
    yql/essentials/providers/common/metrics
    yql/essentials/public/issue
    yql/essentials/utils/failure_injector
    yql/essentials/utils/log
)

END()

RECURSE_FOR_TESTS(
    ut
)
