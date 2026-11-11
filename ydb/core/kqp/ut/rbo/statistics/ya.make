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
    ydb/core/statistics/ut_common
    ydb/library/yql/udfs/statistics_internal
    yql/essentials/core/minsketch
    yql/essentials/udfs/common/hyperloglog
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
)

YQL_LAST_ABI_VERSION()

END()
