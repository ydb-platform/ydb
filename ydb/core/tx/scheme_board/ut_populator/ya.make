UNITTEST_FOR(ydb/core/tx/scheme_board)

FORK_SUBTESTS()

SIZE(MEDIUM)

TIMEOUT(600)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/default
    ydb/core/tx/schemeshard
    ydb/core/tx/schemeshard/ut_helpers
    ydb/core/tx/tx_allocator
)

SRCS(
    populator_ut.cpp
    ut_helpers.cpp
)

YQL_LAST_ABI_VERSION()

END()
