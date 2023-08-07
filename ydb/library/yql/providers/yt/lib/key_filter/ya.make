LIBRARY()

SRCS(
    yql_key_filter.cpp
)

PEERDIR(
    ydb/library/yql/core/expr_nodes
    ydb/library/yql/core
    ydb/library/yql/utils
    ydb/library/yql/ast
)

YQL_LAST_ABI_VERSION()

END()
