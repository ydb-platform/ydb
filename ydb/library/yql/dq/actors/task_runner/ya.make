LIBRARY()

SRCS(
    events.cpp
    task_runner_actor_local.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/yql/dq/runtime
    ydb/library/yql/dq/common
    ydb/library/yql/dq/proto
    ydb/library/yql/minikql/computation
    ydb/library/yql/utils/actors
    ydb/library/services
)

YQL_LAST_ABI_VERSION()

END()
