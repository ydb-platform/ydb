LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

BISON_FLAGS(-Wcounterexamples)

SRCS(
    ast.cpp
    ast_visitors.cpp
    constraints.cpp
    coordination_helpers.cpp
    expr_builder_base.cpp
    expr_builder_v1.cpp
    expr_builder_v2.cpp
    functions.cpp
    helpers.cpp
    hierarchical_join_rewriter.cpp
    builtin_function_registry.cpp
    builtin_function_types.cpp
    functions_common.cpp
    key_trie.cpp
    lexer.rl6
    parser.ypp
    public.cpp
    push_down_group_by.cpp
    query.cpp
    join_profiler.cpp
    query_common.cpp
    query_helpers.cpp
    query_preparer.cpp
    query_visitors.cpp
    typing.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/client
    yt/yt/library/query/misc
    yt/yt/library/query/proto
)

END()

RECURSE_FOR_TESTS(
    unittests
)
