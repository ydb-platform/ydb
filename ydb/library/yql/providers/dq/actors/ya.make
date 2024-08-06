LIBRARY()

SRCS(
    compute_actor.cpp
    dummy_lock.cpp
    dynamic_nameserver.cpp
    events.cpp
    executer_actor.cpp
    execution_helpers.cpp
    graph_execution_events_actor.cpp
    resource_allocator.cpp
    task_controller.cpp
    worker_actor.cpp
    result_aggregator.cpp
    result_receiver.cpp
    full_result_writer.cpp
    proto_builder.cpp
    grouped_issues.cpp
)

PEERDIR(
    ydb/library/actors/core
    library/cpp/yson
    ydb/library/mkql_proto
    ydb/library/yql/core/services
    ydb/library/yql/core/services/mounts
    ydb/library/yql/core/user_data
    ydb/library/yql/core
    ydb/library/yql/utils/actors
    ydb/library/yql/utils/actor_log
    ydb/library/yql/utils/backtrace
    ydb/library/yql/utils/log
    ydb/public/api/protos
    ydb/public/lib/yson_value
    ydb/library/yql/dq/common
    ydb/library/yql/dq/proto
    ydb/library/yql/dq/runtime
    ydb/library/yql/dq/tasks
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/utils/failure_injector
    ydb/library/yql/providers/common/metrics
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
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    events
    yt
)

RECURSE_FOR_TESTS(
    ut
)
