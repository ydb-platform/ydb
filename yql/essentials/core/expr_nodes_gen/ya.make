LIBRARY()

SRCS(
    yql_expr_nodes_gen.h
    yql_expr_nodes_gen.cpp
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/public/udf
)

END()

RECURSE(
    gen
)

