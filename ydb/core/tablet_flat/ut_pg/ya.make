UNITTEST_FOR(ydb/core/tablet_flat)

FORK_SUBTESTS()

SRCS(
    flat_database_pg_ut.cpp
)

PEERDIR(
    ydb/core/scheme
    ydb/core/tablet_flat/test/libs/table
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg
    ydb/library/yql/parser/pg_wrapper
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

END()
