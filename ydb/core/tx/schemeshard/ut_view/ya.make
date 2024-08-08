UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

TIMEOUT(600)
SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

PEERDIR(
    ydb/core/testlib/basics/default
    ydb/core/tx/schemeshard/ut_helpers
)

SRCS(
    ut_view.cpp
)

YQL_LAST_ABI_VERSION()

END()
