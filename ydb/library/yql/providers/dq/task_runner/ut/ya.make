UNITTEST_FOR(ydb/library/yql/providers/dq/task_runner)

NO_BUILD_IF(OS_WINDOWS)

PEERDIR(
    ydb/library/yql/providers/dq/common
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    yql/essentials/utils/failure_injector
)

YQL_LAST_ABI_VERSION()

SRCS(
    tasks_runner_pipe_constructor_ut.cpp
    tasks_runner_pipe_metrics_ut.cpp
    tasks_runner_pipe_process_ut.cpp
)

END()
