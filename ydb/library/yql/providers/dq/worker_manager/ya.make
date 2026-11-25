YQL_LIBRARY()

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/helpers
    ydb/library/actors/interconnect
    ydb/library/ydb_issue
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/common
    ydb/library/yql/providers/dq/actors
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/dq/runtime
    ydb/library/yql/providers/dq/task_runner
    ydb/library/yql/providers/dq/worker_manager/interface
    ydb/library/yql/utils/actors
    yql/essentials/providers/common/config
    yql/essentials/providers/common/gateway
    yql/essentials/providers/common/metrics
    yql/essentials/utils/failure_injector
    yql/essentials/utils/log
)

SRCS(
    local_worker_manager.cpp
)

END()

RECURSE(
    interface
)

RECURSE_FOR_TESTS(
    ut
)
