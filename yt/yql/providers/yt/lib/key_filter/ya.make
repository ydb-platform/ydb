LIBRARY()

SRCS(
    yql_key_filter.cpp
)

PEERDIR(
    yql/essentials/core/expr_nodes
    yql/essentials/core
    yql/essentials/utils
    yql/essentials/ast
)

YQL_LAST_ABI_VERSION()

END()
