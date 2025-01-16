LIBRARY()

SRCS(
    yql_solomon_expr_nodes.cpp
)

PEERDIR(
    yql/essentials/core/expr_nodes
    yql/essentials/providers/common/provider
)

SRCDIR(
    yql/essentials/core/expr_nodes_gen
)

IF(EXPORT_CMAKE)
    RUN_PYTHON3(
        ${ARCADIA_ROOT}/yql/essentials/core/expr_nodes_gen/gen/__main__.py
            yql_expr_nodes_gen.jnj
            yql_solomon_expr_nodes.json
            yql_solomon_expr_nodes.gen.h
            yql_solomon_expr_nodes.decl.inl.h
            yql_solomon_expr_nodes.defs.inl.h
        IN yql_expr_nodes_gen.jnj
        IN yql_solomon_expr_nodes.json
        OUT yql_solomon_expr_nodes.gen.h
        OUT yql_solomon_expr_nodes.decl.inl.h
        OUT yql_solomon_expr_nodes.defs.inl.h
        OUTPUT_INCLUDES
        ${ARCADIA_ROOT}/yql/essentials/core/expr_nodes_gen/yql_expr_nodes_gen.h
        ${ARCADIA_ROOT}/util/generic/hash_set.h
    )
ELSE()
    RUN_PROGRAM(
        yql/essentials/core/expr_nodes_gen/gen
            yql_expr_nodes_gen.jnj
            yql_solomon_expr_nodes.json
            yql_solomon_expr_nodes.gen.h
            yql_solomon_expr_nodes.decl.inl.h
            yql_solomon_expr_nodes.defs.inl.h
        IN yql_expr_nodes_gen.jnj
        IN yql_solomon_expr_nodes.json
        OUT yql_solomon_expr_nodes.gen.h
        OUT yql_solomon_expr_nodes.decl.inl.h
        OUT yql_solomon_expr_nodes.defs.inl.h
        OUTPUT_INCLUDES
        ${ARCADIA_ROOT}/yql/essentials/core/expr_nodes_gen/yql_expr_nodes_gen.h
        ${ARCADIA_ROOT}/util/generic/hash_set.h
    )
ENDIF()

END()
