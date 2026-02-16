UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    kqp_rbo_pg_ut.cpp
    kqp_rbo_yql_ut.cpp
)

PEERDIR(
    library/cpp/resource
    ydb/core/kqp/ut/common
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
    ydb/library/benchmarks/queries/tpch
    ydb/public/lib/ut_helpers
    ydb/library/yql/udfs/statistics_internal
    ydb/core/statistics/ut_common
    yql/essentials/udfs/common/digest
    yql/essentials/udfs/common/hyperloglog
)

ADDINCL(
    yql/essentials/parser/pg_wrapper/postgresql/src/include
)

DATA (
    arcadia/ydb/core/kqp/ut/join/data
    arcadia/ydb/core/kqp/ut/rbo/data
)

RESOURCE(
    ydb/library/benchmarks/gen_queries/consts.yql consts.yql
    ydb/library/benchmarks/gen_queries/consts_decimal.yql consts_decimal.yql
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
