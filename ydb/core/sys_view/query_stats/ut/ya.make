UNITTEST_FOR(ydb/core/sys_view/query_stats)

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

TIMEOUT(600)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    query_stats_ut.cpp
)

END()
