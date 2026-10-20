UNITTEST_FOR(ydb/library/yql/dq/task_scheduler)

SRCS(
    task_scheduler_ut.cpp
    worker_pool_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/library/actors/testlib
)

FORK_SUBTESTS()
SIZE(MEDIUM)

END()
