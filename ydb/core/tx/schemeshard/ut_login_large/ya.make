UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

TAG(ya:manual)

PEERDIR(
    ydb/core/testlib/default
    ydb/core/tx
    ydb/core/tx/schemeshard/ut_helpers
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_login_large.cpp
)

END()
