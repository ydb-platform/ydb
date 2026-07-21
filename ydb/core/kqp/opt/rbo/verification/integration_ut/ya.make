UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

SRCS(
    optimizer_snapshot_pair_ut.cpp
)

DEPENDS(
    ydb/core/kqp/opt/rbo/verification/bin
)

PEERDIR(
    library/cpp/testing/common
    ydb/core/kqp/ut/common
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
