LIBRARY()

SRCS(
    dq_function_expr_nodes.cpp
)

PEERDIR(
    yql/essentials/core/expr_nodes

)

SRCDIR(
    yql/essentials/core/expr_nodes_gen
)

RUN_PY3_PROGRAM(
    yql/essentials/core/expr_nodes_gen/gen
        yql_expr_nodes_gen.jnj
        dq_function_expr_nodes.json
        dq_function_expr_nodes.gen.h
        dq_function_expr_nodes.decl.inl.h
        dq_function_expr_nodes.defs.inl.h
    IN yql_expr_nodes_gen.jnj
    IN dq_function_expr_nodes.json
    OUT dq_function_expr_nodes.gen.h
    OUT dq_function_expr_nodes.decl.inl.h
    OUT dq_function_expr_nodes.defs.inl.h
    OUTPUT_INCLUDES
    ${ARCADIA_ROOT}/yql/essentials/core/expr_nodes_gen/yql_expr_nodes_gen.h
    ${ARCADIA_ROOT}/util/generic/hash_set.h
)

END()
