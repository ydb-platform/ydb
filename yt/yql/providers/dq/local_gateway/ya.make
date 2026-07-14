LIBRARY()

YQL_LAST_ABI_VERSION()

SRCS(
    yql_dq_gateway_local.cpp
)

PEERDIR(
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/actors/spilling
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/dq/provider
    ydb/library/yql/providers/dq/task_runner
    ydb/library/yql/providers/dq/worker_manager
    yql/essentials/utils
    yql/essentials/utils/network
    yt/yql/providers/dq/gateway
    yt/yql/providers/dq/service
    yt/yql/providers/dq/stats_collector
)

END()

RECURSE_FOR_TESTS(
    ut
)
