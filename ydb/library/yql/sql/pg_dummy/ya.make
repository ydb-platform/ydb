LIBRARY()

OWNER(g:yql)

PEERDIR(
    ydb/library/yql/ast
    ydb/library/yql/sql/settings
    ydb/library/yql/providers/common/codec
)

SRCS(
    pg_sql_dummy.cpp
)

YQL_LAST_ABI_VERSION()

END()
