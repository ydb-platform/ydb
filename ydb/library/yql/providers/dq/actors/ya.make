LIBRARY()

SRCS(
    compute_actor.cpp
    events.cpp
    executer_actor.cpp
    execution_helpers.cpp
    full_result_writer.cpp
    graph_execution_events_actor.cpp
    grouped_issues.cpp
    proto_builder.cpp
    resource_allocator.cpp
    result_aggregator.cpp
    result_receiver.cpp
    task_controller.cpp
    worker_actor.cpp
)

PEERDIR(
    library/cpp/yson
    ydb/library/actors/core
    ydb/library/mkql_proto
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/common
    ydb/library/yql/dq/proto
    ydb/library/yql/dq/runtime
    ydb/library/yql/dq/tasks
    ydb/library/yql/providers/dq/actors/events
    ydb/library/yql/providers/dq/api/grpc
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/dq/common
    ydb/library/yql/providers/dq/config
    ydb/library/yql/providers/dq/counters
    ydb/library/yql/providers/dq/interface
    ydb/library/yql/providers/dq/planner
    ydb/library/yql/providers/dq/task_runner
    ydb/library/yql/providers/dq/task_runner_actor
    ydb/library/yql/providers/dq/worker_manager
    ydb/library/yql/providers/dq/worker_manager/interface
    ydb/library/yql/utils/actors
    ydb/library/yql/utils/actor_log
    ydb/public/api/protos
    ydb/public/lib/yson_value
    yql/essentials/core
    yql/essentials/providers/common/metrics
    yql/essentials/utils/failure_injector
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    events
)

RECURSE_FOR_TESTS(
    ut
)
