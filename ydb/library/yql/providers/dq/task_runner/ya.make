LIBRARY()

PEERDIR(
    library/cpp/yson/node
    yql/essentials/minikql/invoke_builtins
    yql/essentials/utils
    ydb/library/yql/dq/runtime
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/dq/counters
)

YQL_LAST_ABI_VERSION()

SRCS(
    file_cache.cpp
    tasks_runner_local.cpp
    tasks_runner_proxy.cpp
)

END()
