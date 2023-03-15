LIBRARY()

PROVIDES(
    yql_pg_sql_translator
)

PEERDIR(
    ydb/library/yql/ast
    ydb/library/yql/core
    ydb/library/yql/parser/pg_catalog
    ydb/library/yql/parser/pg_wrapper
    ydb/library/yql/sql/settings
)

ADDINCL(
    ydb/library/yql/parser/pg_wrapper/postgresql/src/include
)

SRCS(
    pg_sql.cpp
    utils.cpp
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

RECURSE_FOR_TESTS(
    ut
)
