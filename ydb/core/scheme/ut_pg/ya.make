UNITTEST_FOR(ydb/core/scheme)

FORK_SUBTESTS()

SRCS(
    scheme_tablecell_pg_ut.cpp
)

PEERDIR(
    ydb/core/scheme
    yql/essentials/public/udf/service/exception_policy
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
