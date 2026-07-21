UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()

SIZE(LARGE)
TAG(ya:fat)
REQUIREMENTS(cpu:2)

SRCS(
    benchmark_coverage_ut.cpp
)

DEPENDS(
    ydb/core/kqp/opt/rbo/verification/bin
)

PEERDIR(
    library/cpp/testing/common
    ydb/core/kqp/ut/common
    yql/essentials/sql/pg_dummy
)

DATA(
    arcadia/ydb/core/kqp/ut/rbo/data
)

YQL_LAST_ABI_VERSION()

END()
