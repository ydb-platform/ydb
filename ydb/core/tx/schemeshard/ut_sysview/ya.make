UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    ydb/core/testlib/basics/default
    ydb/core/tx/schemeshard/ut_helpers
)

SRCS(
    ut_sysview.cpp
)

YQL_LAST_ABI_VERSION()

END()
