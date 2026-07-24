UNITTEST_FOR(ydb/core/kqp)

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)
TAG(ya:manual)

SRCS(
    decimal_sum_runtime_ut.cpp
    string_in_runtime_ut.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
    ydb/core/tx/sharding
    yql/essentials/parser/pg_wrapper
    yql/essentials/sql/pg
)

YQL_LAST_ABI_VERSION()

END()
