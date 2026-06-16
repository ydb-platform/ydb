LIBRARY()

PEERDIR(
    yql/essentials/parser/antlr_ast/gen/v1_ansi_antlr4
    yql/essentials/parser/antlr_ast/gen/v1_antlr4
)

SRCS(
    base_visitor.cpp
    cursor_text.cpp
    narrowing_visitor.cpp
    parse_tree.cpp
    parser.cpp
)

END()

RECURSE_FOR_TESTS(

)
