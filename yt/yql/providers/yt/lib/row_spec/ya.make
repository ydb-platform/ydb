LIBRARY()

SRCS(
    yql_row_spec.cpp
)

PEERDIR(
    library/cpp/yson/node
    yql/essentials/ast
    yql/essentials/core/expr_nodes_gen
    yql/essentials/core
    yql/essentials/core/expr_nodes
    yql/essentials/core/issue
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/schema
    yql/essentials/providers/common/schema/expr
    yt/yql/providers/yt/common
    yt/yql/providers/yt/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()
