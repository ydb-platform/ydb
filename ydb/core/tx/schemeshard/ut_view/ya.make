UNITTEST_FOR(ydb/core/tx/schemeshard)

SIZE(MEDIUM)

PEERDIR(
    ydb/core/testlib/basics/default
    ydb/core/tx/schemeshard/ut_helpers
)

SRCS(
    ut_view.cpp
)

YQL_LAST_ABI_VERSION()

END()
