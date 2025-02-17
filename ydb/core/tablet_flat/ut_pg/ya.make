UNITTEST_FOR(ydb/core/tablet_flat)

FORK_SUBTESTS()

SRCS(
    flat_database_pg_ut.cpp
)

PEERDIR(
    ydb/core/scheme
    ydb/core/tablet_flat/test/libs/table
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
)

ADDINCL(
    yql/essentials/parser/pg_wrapper/postgresql/src/include
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
