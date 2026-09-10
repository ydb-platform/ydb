UNITTEST_FOR(ydb/library/yql/providers/dq/task_runner)

SRCS(
    tasks_runner_pipe_process_ut.cpp
    tasks_runner_pipe_pool_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
)

YQL_LAST_ABI_VERSION()

END()
