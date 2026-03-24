UNITTEST_FOR(ydb/core/kqp/rm_service)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    kqp_rm_ut.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
