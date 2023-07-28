LIBRARY()

SRCS(
    yql_graph_reorder.cpp
    yql_graph_reorder_old.cpp
)

PEERDIR(
    ydb/library/yql/ast
    ydb/library/yql/utils/log
    ydb/library/yql/core
    ydb/library/yql/core/expr_nodes
    ydb/library/yql/providers/common/provider
)

YQL_LAST_ABI_VERSION()

END()
