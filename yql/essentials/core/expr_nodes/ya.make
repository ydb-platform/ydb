LIBRARY()

SRCS(
    yql_expr_nodes.h
    yql_expr_nodes.cpp
)

PEERDIR(
    yql/essentials/core/expr_nodes_gen
    yql/essentials/public/udf
    yql/essentials/core/issue
)

SRCDIR(yql/essentials/core/expr_nodes_gen)

RUN_PY3_PROGRAM(
    yql/essentials/core/expr_nodes_gen/gen
        yql_expr_nodes_gen.jnj
        yql_expr_nodes.json
        yql_expr_nodes.gen.h
        yql_expr_nodes.decl.inl.h
        yql_expr_nodes.defs.inl.h
    IN yql_expr_nodes_gen.jnj
    IN yql_expr_nodes.json
    OUT yql_expr_nodes.gen.h
    OUT yql_expr_nodes.decl.inl.h
    OUT yql_expr_nodes.defs.inl.h
    OUTPUT_INCLUDES
    ${ARCADIA_ROOT}/yql/essentials/core/expr_nodes_gen/yql_expr_nodes_gen.h
    ${ARCADIA_ROOT}/util/generic/hash_set.h
)

END()
