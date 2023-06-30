LIBRARY()

SRCS(
    yql_yt_join.cpp
    yql_yt_key_selector.cpp
)

PEERDIR(
    ydb/library/yql/providers/yt/lib/row_spec
    ydb/library/yql/core/expr_nodes
    ydb/library/yql/core
    ydb/library/yql/ast
)


   YQL_LAST_ABI_VERSION()


END()
