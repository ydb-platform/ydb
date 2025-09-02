LIBRARY()

SRCS(
    yql_graph_reorder.cpp
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/utils/log
    yql/essentials/core
    yql/essentials/core/expr_nodes
    yql/essentials/providers/common/provider
)

YQL_LAST_ABI_VERSION()

END()
