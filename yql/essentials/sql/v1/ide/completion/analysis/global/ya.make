LIBRARY()

SRCS(
    column.cpp
    evaluate.cpp
    function.cpp
    global.cpp
    input.cpp
    named_node_resolution.cpp
    named_node_visibility.cpp
    parse_tree.cpp
    parser.cpp
    use.cpp
)

PEERDIR(
    yql/essentials/sql/v1/ide/completion/core
    yql/essentials/sql/v1/ide/completion/syntax
    yql/essentials/sql/v1/ide/completion/text
    yql/essentials/sql/v1/ide/pure_ast
    yql/essentials/parser/antlr_ast/gen/v1_antlr4
    yql/essentials/parser/antlr_ast/gen/v1_ansi_antlr4
    contrib/libs/re2
)

END()

RECURSE_FOR_TESTS(
    ut
)
