LIBRARY()

SRCS(
    yql_dq_integration.cpp
    yql_dq_optimization.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/yson
    yql/essentials/ast
    yql/essentials/core
    yql/essentials/core/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    transform
)
