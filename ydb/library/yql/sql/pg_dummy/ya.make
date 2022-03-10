LIBRARY()

OWNER(g:yql)

PEERDIR(
    ydb/library/yql/ast
    ydb/library/yql/sql/settings
    ydb/library/yql/providers/common/codec
    ydb/library/yql/minikql/computation
    ydb/library/yql/core
)

SRCS(
    pg_sql_dummy.cpp
)

YQL_LAST_ABI_VERSION()

END()
