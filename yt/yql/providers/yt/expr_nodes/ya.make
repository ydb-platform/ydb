LIBRARY()

SRCS(
    yql_yt_expr_nodes.cpp
)

PEERDIR(
    yql/essentials/core/expr_nodes
    yql/essentials/providers/common/provider
)

SRCDIR(
    yql/essentials/core/expr_nodes_gen
)

RUN_PY3_PROGRAM(
    yql/essentials/core/expr_nodes_gen/gen
        yql_expr_nodes_gen.jnj
        yql_yt_expr_nodes.json
        yql_yt_expr_nodes.gen.h
        yql_yt_expr_nodes.decl.inl.h
        yql_yt_expr_nodes.defs.inl.h
    IN yql_expr_nodes_gen.jnj
    IN yql_yt_expr_nodes.json
    OUT yql_yt_expr_nodes.gen.h
    OUT yql_yt_expr_nodes.decl.inl.h
    OUT yql_yt_expr_nodes.defs.inl.h
    OUTPUT_INCLUDES
    ${ARCADIA_ROOT}/yql/essentials/core/expr_nodes_gen/yql_expr_nodes_gen.h
    ${ARCADIA_ROOT}/util/generic/hash_set.h
)

END()
