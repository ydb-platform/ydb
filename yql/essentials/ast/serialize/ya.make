LIBRARY()

ENABLE(YQL_STYLE_CPP)

SRCS(
    yql_expr_serialize.cpp
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/core/issue
    yql/essentials/minikql
)

END()
