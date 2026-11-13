UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
TIMEOUT(1800)
REQUIREMENTS(cpu:1)
REQUIREMENTS(ram:8)

DEPENDS(
    ydb/apps/ydb
)

SRCS(
    kqp_analyzed_join_order_ut.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
    ydb/core/kqp/ut/rbo/statistics/helpers
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
)

DATA(
    arcadia/ydb/core/kqp/ut/rbo/statistics/data
    arcadia/ydb/core/kqp/ut/rbo/data/yql-tpch
    arcadia/ydb/core/kqp/ut/rbo/data/yql-tpcds
    arcadia/ydb/core/kqp/ut/rbo/statistics/join_order/canondata
)

YQL_LAST_ABI_VERSION()

END()
