YQL_LIBRARY()

SRCS(
    yql_yt_join.cpp
    yql_yt_key_selector.cpp
)

PEERDIR(
    yt/yql/providers/yt/lib/row_spec
    yql/essentials/core/expr_nodes
    yql/essentials/core
    yql/essentials/ast
)

END()
