LIBRARY()

SRCS(
    yql_expr_nodes_gen.h
    yql_expr_nodes_gen.cpp
)

PEERDIR(
    ydb/library/yql/ast
    ydb/library/yql/public/udf
)

END()
