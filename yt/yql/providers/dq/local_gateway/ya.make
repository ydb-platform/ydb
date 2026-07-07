LIBRARY()

YQL_LAST_ABI_VERSION()

SRCS(
    yql_dq_gateway_local.cpp
)

PEERDIR(
    yql/essentials/utils
    yql/essentials/utils/network
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/dq/actors/spilling
    contrib/ydb/library/yql/providers/dq/provider
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/dq/task_runner
    contrib/ydb/library/yql/providers/dq/worker_manager
    yt/yql/providers/dq/service
    yt/yql/providers/dq/stats_collector
)

END()

RECURSE_FOR_TESTS(
    ut
)
