UNITTEST_FOR(ydb/library/yql/dq/task_scheduler)

SRCS(comparison_ut.cpp)

PEERDIR(
    library/cpp/testing/common
    library/cpp/testing/unittest
    ydb/library/actors/testlib
)

FORK_SUBTESTS()
SIZE(MEDIUM)

END()
