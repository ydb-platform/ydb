LIBRARY()

SRCS(
    evaluate.cpp
    global.cpp
    named_node.cpp
    narrowing_visitor.cpp
    parse_tree.cpp
    use.cpp
)

PEERDIR(
    yql/essentials/sql/v1/complete/core
    yql/essentials/sql/v1/complete/syntax
    yql/essentials/parser/antlr_ast/gen/v1_antlr4
    yql/essentials/parser/antlr_ast/gen/v1_ansi_antlr4
)

END()

RECURSE_FOR_TESTS(
    ut
)
