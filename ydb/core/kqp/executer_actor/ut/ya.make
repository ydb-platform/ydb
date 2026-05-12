UNITTEST_FOR(ydb/core/kqp/executer_actor)

SIZE(MEDIUM)

SRCS(
    kqp_executer_ut.cpp
)

PEERDIR(
    ydb/core/kqp/common
    ydb/core/kqp/ut/common
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
