LIBRARY()

SRCS(
    ansi.cpp
    grammar.cpp
    local.cpp
    parser_call_stack.cpp
)

ADDINCL(
    yql/essentials/sql/v1/complete
)

PEERDIR(
    yql/essentials/core/issue

    yql/essentials/parser/antlr_ast/gen/v1_ansi_antlr4
    yql/essentials/parser/antlr_ast/gen/v1_antlr4
    yql/essentials/parser/lexer_common

    yql/essentials/sql/settings
    yql/essentials/sql/v1/lexer
    yql/essentials/sql/v1/reflect
)

END()

RECURSE_FOR_TESTS(
    ut
)
