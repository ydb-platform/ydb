LIBRARY()

PROVIDES(
    yql_pg_sql_translator
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/core/sql_types
    yql/essentials/parser/pg_catalog
    yql/essentials/minikql
    yql/essentials/sql/settings
)

ADDINCL(
    yql/essentials/parser/pg_wrapper/postgresql/src/include
)

SRCS(
    pg_sql.cpp
    optimizer.cpp
    utils.cpp
)

CFLAGS(
    -Dpalloc0=yql_palloc0
    -Dpfree=yql_pfree
)

IF (OS_WINDOWS)
CFLAGS(
   "-D__thread=__declspec(thread)"
   -Dfstat=microsoft_native_fstat
   -Dstat=microsoft_native_stat
)
ENDIF()

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
