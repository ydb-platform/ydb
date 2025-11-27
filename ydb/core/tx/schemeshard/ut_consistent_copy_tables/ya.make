UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)

PEERDIR(
    ydb/core/tx/schemeshard/ut_helpers
)

SRCS(
    ut_consistent_copy_tables.cpp
)

YQL_LAST_ABI_VERSION()

END()
