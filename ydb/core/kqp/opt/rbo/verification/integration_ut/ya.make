UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

SRCS(
    optimizer_snapshot_pair_ut.cpp
)

DEPENDS(
    contrib/tools/z3
    ydb/core/kqp/opt/rbo/verification/bin
)

PEERDIR(
    library/cpp/testing/common
    ydb/core/kqp/ut/common
    yql/essentials/parser/pg_wrapper
    yql/essentials/sql/pg
)

DATA(
    arcadia/ydb/core/kqp/ut/rbo/data
)

YQL_LAST_ABI_VERSION()

END()
