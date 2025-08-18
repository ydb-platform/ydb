LIBRARY()

SRCS(
    base_visitor.cpp
    column.cpp
    evaluate.cpp
    function.cpp
    global.cpp
    input.cpp
    named_node.cpp
    narrowing_visitor.cpp
    parse_tree.cpp
    use.cpp
)

PEERDIR(
    yql/essentials/sql/v1/complete/core
    yql/essentials/sql/v1/complete/syntax
    yql/essentials/sql/v1/complete/text
    yql/essentials/parser/antlr_ast/gen/v1_antlr4
    yql/essentials/parser/antlr_ast/gen/v1_ansi_antlr4
    contrib/libs/re2
)

END()

RECURSE_FOR_TESTS(
    ut
)
