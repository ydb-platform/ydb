LIBRARY()

PEERDIR(
    ydb/library/actors/core
    ydb/library/ydb_issue
    yql/essentials/utils/failure_injector
    yql/essentials/providers/common/config
    yql/essentials/providers/common/gateway
    yql/essentials/providers/common/metrics
    ydb/library/yql/providers/dq/api/grpc
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/dq/runtime
    ydb/library/yql/providers/dq/task_runner
    ydb/library/yql/providers/dq/task_runner_actor
    ydb/library/yql/providers/dq/worker_manager/interface
)

YQL_LAST_ABI_VERSION()

SRCS(
    local_worker_manager.cpp
)

END()

RECURSE(
    interface
)
