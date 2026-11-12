UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
REQUIREMENTS(ram:8)

DEPENDS(
    ydb/apps/ydb
)

SRCS(
    kqp_rbo_statistics_ut.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
    ydb/core/kqp/ut/rbo/statistics/helpers
    yql/essentials/core/minsketch
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
)

DATA(
    arcadia/ydb/core/kqp/ut/rbo/statistics/data
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    correctness
    join_order
)
