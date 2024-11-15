LIBRARY()

SRCS(
    dq_expr_nodes.h
)

PEERDIR(
    yql/essentials/core/expr_nodes
)

SRCDIR(
    yql/essentials/core/expr_nodes_gen
)

IF(EXPORT_CMAKE)
    RUN_PYTHON3(
        ${ARCADIA_ROOT}/yql/essentials/core/expr_nodes_gen/gen/__main__.py
        yql_expr_nodes_gen.jnj
        dq_expr_nodes.json
        dq_expr_nodes.gen.h
        dq_expr_nodes.decl.inl.h
        dq_expr_nodes.defs.inl.h
        IN yql_expr_nodes_gen.jnj
        IN dq_expr_nodes.json
        OUT dq_expr_nodes.gen.h
        OUT dq_expr_nodes.decl.inl.h
        OUT dq_expr_nodes.defs.inl.h
        OUTPUT_INCLUDES
        ${ARCADIA_ROOT}/yql/essentials/core/expr_nodes_gen/yql_expr_nodes_gen.h
        ${ARCADIA_ROOT}/util/generic/hash_set.h
    )
ELSE()
    RUN_PROGRAM(
        yql/essentials/core/expr_nodes_gen/gen
        yql_expr_nodes_gen.jnj
        dq_expr_nodes.json
        dq_expr_nodes.gen.h
        dq_expr_nodes.decl.inl.h
        dq_expr_nodes.defs.inl.h
        IN yql_expr_nodes_gen.jnj
        IN dq_expr_nodes.json
        OUT dq_expr_nodes.gen.h
        OUT dq_expr_nodes.decl.inl.h
        OUT dq_expr_nodes.defs.inl.h
        OUTPUT_INCLUDES
        ${ARCADIA_ROOT}/yql/essentials/core/expr_nodes_gen/yql_expr_nodes_gen.h
        ${ARCADIA_ROOT}/util/generic/hash_set.h
    )
ENDIF()

END()
