LIBRARY()

SRCS(
    events.cpp
    task_runner_actor_local.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/library/yql/dq/runtime
    ydb/library/yql/dq/common
    ydb/library/yql/dq/proto
    ydb/library/yql/minikql/computation/llvm
    ydb/library/yql/utils/actors
    ydb/core/protos
)

YQL_LAST_ABI_VERSION()

END()
