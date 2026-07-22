UNITTEST_FOR(ydb/core/kqp)

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)
TAG(ya:manual)

SRCS(
    decimal_sum_runtime_ut.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
    ydb/core/tx/sharding
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
