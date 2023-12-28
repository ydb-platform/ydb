LIBRARY()

PEERDIR(
    library/cpp/svnversion
    library/cpp/threading/task_scheduler
    library/cpp/yson/node
    ydb/library/yql/dq/integration/transform
    ydb/library/yql/minikql/invoke_builtins
    ydb/library/yql/protos
    ydb/library/yql/utils
    ydb/library/yql/utils/backtrace
    ydb/library/yql/utils/log
    ydb/library/yql/dq/proto
    ydb/library/yql/dq/runtime
    ydb/library/yql/providers/common/proto
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/dq/counters
)

YQL_LAST_ABI_VERSION()

SRCS(
    file_cache.cpp
    tasks_runner_local.cpp
    tasks_runner_proxy.cpp
    tasks_runner_pipe.cpp
)

END()
