LIBRARY()

OWNER(g:yql)

PEERDIR(
    ydb/library/yql/ast
    ydb/library/yql/parser/pg_wrapper
    ydb/library/yql/sql/settings
)

ADDINCL(
    ydb/library/yql/parser/pg_wrapper/postgresql/src/include
)

SRCS(
    pg_sql.cpp
)

IF (OS_WINDOWS)
CFLAGS(
   "-D__thread=__declspec(thread)"
   -Dfstat=microsoft_native_fstat
   -Dstat=microsoft_native_stat
)
ENDIF()

NO_COMPILER_WARNINGS()

END()
