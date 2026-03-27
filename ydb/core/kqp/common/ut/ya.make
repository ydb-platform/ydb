UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()

SIZE(SMALL)

SRCS(
    kqp_tli_ut.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
