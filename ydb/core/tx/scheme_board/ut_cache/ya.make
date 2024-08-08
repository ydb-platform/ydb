UNITTEST_FOR(ydb/core/tx/scheme_board)

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

TIMEOUT(600)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/default
    ydb/core/tx/schemeshard
    ydb/core/tx/schemeshard/ut_helpers
)

YQL_LAST_ABI_VERSION()

SRCS(
    cache_ut.cpp
    ut_helpers.cpp
)

END()
