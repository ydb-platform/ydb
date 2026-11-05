UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(16)

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

SRCS(
    kqp_rbo_olap_distinct_ut.cpp
    kqp_rbo_olap_ut.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
    ydb/core/kqp/ut/olap/helpers
    ydb/core/statistics/ut_common
    ydb/library/plan2svg
    ydb/public/lib/ut_helpers
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
    yql/essentials/udfs/common/digest
)

IF (OS_WINDOWS)
CFLAGS(
   "-D__thread=__declspec(thread)"
   -Dfstat=microsoft_native_fstat
   -Dstat=microsoft_native_stat
)
ENDIF()

NO_COMPILER_WARNINGS()

YQL_LAST_ABI_VERSION()

END()
