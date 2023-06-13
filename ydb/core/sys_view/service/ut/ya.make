UNITTEST_FOR(ydb/core/sys_view/service)

FORK_SUBTESTS()
SIZE(MEDIUM)
TIMEOUT(600)

PEERDIR(
    library/cpp/testing/unittest
)

SRCS(
    query_history_ut.cpp
)

END()
