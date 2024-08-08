UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

SRCS(
    kqp_pg_ut.cpp
    pg_catalog_ut.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg
    ydb/library/yql/parser/pg_wrapper
    ydb/public/lib/ut_helpers
)

ADDINCL(
    ydb/library/yql/parser/pg_wrapper/postgresql/src/include
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

REQUIREMENTS(ram:32)

END()
