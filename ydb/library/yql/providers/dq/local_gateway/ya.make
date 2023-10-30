LIBRARY()

YQL_LAST_ABI_VERSION()

SRCS(
    yql_dq_gateway_local.cpp
)

PEERDIR(
    ydb/library/yql/utils
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/actors/spilling
    ydb/library/yql/providers/dq/provider
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/dq/task_runner
    ydb/library/yql/providers/dq/worker_manager
    ydb/library/yql/providers/dq/service
    ydb/library/yql/providers/dq/stats_collector
)

END()
