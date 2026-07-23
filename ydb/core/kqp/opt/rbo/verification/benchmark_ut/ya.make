UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()

SIZE(LARGE)
TAG(ya:fat)
REQUIREMENTS(cpu:2)

SRCS(
    benchmark_coverage_ut.cpp
)

DEPENDS(
    contrib/tools/z3
    ydb/core/kqp/opt/rbo/verification/bin
)

PEERDIR(
    contrib/libs/openssl
    library/cpp/testing/common
    ydb/core/kqp/ut/common
    yql/essentials/parser/pg_wrapper
    yql/essentials/sql/pg
)

DATA(
    arcadia/ydb/core/kqp/opt/rbo/verification/benchmark_ut/coverage_policy.json
    arcadia/ydb/core/kqp/ut/rbo/data
)

YQL_LAST_ABI_VERSION()

END()
