YQL_LIBRARY()

PEERDIR(
    library/cpp/svnversion
    library/cpp/threading/task_scheduler
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
    file_cache.cpp
    tasks_runner_local.cpp
    tasks_runner_pipe.cpp
    tasks_runner_proxy.cpp
)

IF (OS_WINDOWS)
    SRCS(tasks_runner_pipe_process_win.cpp)
ELSE()
    SRCS(tasks_runner_pipe_process.cpp)
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)
