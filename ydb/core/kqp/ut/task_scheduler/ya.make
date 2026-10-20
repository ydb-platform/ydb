UNITTEST()

SRCS(task_scheduler_ut.cpp)

PEERDIR(
    ydb/core/kqp/compute_actor
    library/cpp/testing/unittest
    ydb/core/kqp/ut/common
    ydb/library/yql/dq/task_scheduler
    yql/essentials/sql/pg_dummy
)

FORK_SUBTESTS()
SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

YQL_LAST_ABI_VERSION()

END()
