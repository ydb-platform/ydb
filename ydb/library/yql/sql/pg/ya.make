LIBRARY()

OWNER(g:yql)

PEERDIR(
    ydb/library/yql/ast
    ydb/library/yql/parser/pg_query_wrapper
    ydb/library/yql/sql/settings
)

ADDINCL(
    ydb/library/yql/parser/pg_query_wrapper/contrib
    GLOBAL ydb/library/yql/parser/pg_query_wrapper/contrib/vendor
    ydb/library/yql/parser/pg_query_wrapper/contrib/src/postgres/include
)

SRCS(
    pg_sql.cpp
)

IF (OS_WINDOWS)
CFLAGS(
   "-D__thread=__declspec(thread)"
)
ENDIF()

END()
