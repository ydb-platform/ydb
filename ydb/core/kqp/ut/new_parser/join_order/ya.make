UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

SRCS(kqp_join_order_ut.cpp)

PEERDIR(
    ydb/core/kqp/ut/common
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
)

# Read the original schemas, statistics, workload SQL and canonical join-order
# JSON from the RBO tree. This target compares them but never canonizes them.
DATA(
    arcadia/ydb/core/kqp/ut/rbo/data
)

YQL_LAST_ABI_VERSION()

END()
