UNITTEST_FOR(ydb/library/yql/providers/dq/task_runner)

PEERDIR(
    ydb/library/yql/providers/dq/common
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    yql/essentials/utils/failure_injector
)

YQL_LAST_ABI_VERSION()

IF (NOT OS_WINDOWS)
    SRCS(tasks_runner_pipe_metrics_ut.cpp)
ENDIF()

END()
