LIBRARY()

OWNER(g:yql)

PEERDIR(
    ydb/library/yql/ast
    ydb/library/yql/sql/settings
)

SRCS(
    pg_sql_dummy.cpp
)

END()
