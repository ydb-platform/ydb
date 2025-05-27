LIBRARY()

SRCS(
    yql_expr_traits.cpp
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/minikql/computation
    yql/essentials/utils/log
    yql/essentials/core
    yql/essentials/core/expr_nodes
    yql/essentials/providers/common/provider
    yt/yql/providers/yt/common
    yt/yql/providers/yt/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()
