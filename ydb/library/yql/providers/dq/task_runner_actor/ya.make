LIBRARY()

OWNER(g:yql)

SRCS(
    task_runner_actor.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/library/yql/dq/actors/task_runner
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/utils/actors
    ydb/library/yql/providers/dq/task_runner
)

YQL_LAST_ABI_VERSION()

END()
