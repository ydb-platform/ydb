UNITTEST_FOR(ydb/core/sys_view/partition_stats)

FORK_SUBTESTS()

SIZE(MEDIUM)

TIMEOUT(600)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    partition_stats_ut.cpp
)

END()
