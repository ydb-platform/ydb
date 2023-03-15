LIBRARY()

SRCS(
    kqp_expr_nodes.h
    kqp_expr_nodes.cpp
)

PEERDIR(
    ydb/library/yql/dq/expr_nodes
)

SRCDIR(ydb/library/yql/core/expr_nodes_gen)

IF(EXPORT_CMAKE)
    RUN_PYTHON3(
        ${ARCADIA_ROOT}/ydb/library/yql/core/expr_nodes_gen/gen/__main__.py
            yql_expr_nodes_gen.jnj
            kqp_expr_nodes.json
            kqp_expr_nodes.gen.h
            kqp_expr_nodes.decl.inl.h
            kqp_expr_nodes.defs.inl.h
        IN yql_expr_nodes_gen.jnj
        IN kqp_expr_nodes.json
        OUT kqp_expr_nodes.gen.h
        OUT kqp_expr_nodes.decl.inl.h
        OUT kqp_expr_nodes.defs.inl.h
        OUTPUT_INCLUDES
        ${ARCADIA_ROOT}/ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.h
        ${ARCADIA_ROOT}/util/generic/hash_set.h
    )
ELSE()
    RUN_PROGRAM(
        ydb/library/yql/core/expr_nodes_gen/gen
            yql_expr_nodes_gen.jnj
            kqp_expr_nodes.json
            kqp_expr_nodes.gen.h
            kqp_expr_nodes.decl.inl.h
            kqp_expr_nodes.defs.inl.h
        IN yql_expr_nodes_gen.jnj
        IN kqp_expr_nodes.json
        OUT kqp_expr_nodes.gen.h
        OUT kqp_expr_nodes.decl.inl.h
        OUT kqp_expr_nodes.defs.inl.h
        OUTPUT_INCLUDES
        ${ARCADIA_ROOT}/ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.h
        ${ARCADIA_ROOT}/util/generic/hash_set.h
    )
ENDIF()

END()
