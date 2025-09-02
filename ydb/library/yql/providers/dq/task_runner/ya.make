LIBRARY()

PEERDIR(
    library/cpp/svnversion
    library/cpp/threading/task_scheduler
    library/cpp/yson/node
    yql/essentials/core/dq_integration/transform
    yql/essentials/minikql/invoke_builtins
    yql/essentials/protos
    yql/essentials/utils
    yql/essentials/utils/backtrace
    yql/essentials/utils/log
    ydb/library/yql/dq/common
    ydb/library/yql/dq/proto
    ydb/library/yql/dq/runtime
    yql/essentials/providers/common/proto
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
