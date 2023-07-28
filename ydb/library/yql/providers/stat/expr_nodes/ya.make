LIBRARY()

PEERDIR(
    ydb/library/yql/core/expr_nodes
    ydb/library/yql/providers/common/provider
)

SRCS(
    yql_stat_expr_nodes.cpp
)

SRCDIR(
    ydb/library/yql/core/expr_nodes_gen
)

IF(EXPORT_CMAKE)
    RUN_PYTHON3(
        ${ARCADIA_ROOT}/ydb/library/yql/core/expr_nodes_gen/gen/__main__.py
            yql_expr_nodes_gen.jnj
            yql_stat_expr_nodes.json
            yql_stat_expr_nodes.gen.h
            yql_stat_expr_nodes.decl.inl.h
            yql_stat_expr_nodes.defs.inl.h
        IN yql_expr_nodes_gen.jnj
        IN yql_stat_expr_nodes.json
        OUT yql_stat_expr_nodes.gen.h
        OUT yql_stat_expr_nodes.decl.inl.h
        OUT yql_stat_expr_nodes.defs.inl.h
        OUTPUT_INCLUDES
        ${ARCADIA_ROOT}/ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.h
        ${ARCADIA_ROOT}/util/generic/hash_set.h
    )
ELSE()
    RUN_PROGRAM(
        ydb/library/yql/core/expr_nodes_gen/gen
            yql_expr_nodes_gen.jnj
            yql_stat_expr_nodes.json
            yql_stat_expr_nodes.gen.h
            yql_stat_expr_nodes.decl.inl.h
            yql_stat_expr_nodes.defs.inl.h
        IN yql_expr_nodes_gen.jnj
        IN yql_stat_expr_nodes.json
        OUT yql_stat_expr_nodes.gen.h
        OUT yql_stat_expr_nodes.decl.inl.h
        OUT yql_stat_expr_nodes.defs.inl.h
        OUTPUT_INCLUDES
        ${ARCADIA_ROOT}/ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.h
        ${ARCADIA_ROOT}/util/generic/hash_set.h
    )
ENDIF()

END()
