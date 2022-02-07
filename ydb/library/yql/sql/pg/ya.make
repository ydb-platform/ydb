LIBRARY()

OWNER(g:yql)

PEERDIR(
    ydb/library/yql/ast
    ydb/library/yql/parser/pg_query_wrapper
    ydb/library/yql/sql/settings
)

SRCS(
    pg_sql.cpp
)

END()
