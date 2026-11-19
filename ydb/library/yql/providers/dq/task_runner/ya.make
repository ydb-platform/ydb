YQL_LIBRARY()

PEERDIR(
    library/cpp/yson/node
    ydb/library/yql/dq/common
    ydb/library/yql/dq/proto
    ydb/library/yql/dq/runtime
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/dq/counters
    yql/essentials/core/dq_integration/transform
    yql/essentials/minikql/invoke_builtins
    yql/essentials/protos
    yql/essentials/providers/common/proto
    yql/essentials/utils
    yql/essentials/utils/backtrace
    yql/essentials/utils/log
)

SRCS(
    tasks_runner_local.cpp
    tasks_runner_proxy.cpp
)

END()
