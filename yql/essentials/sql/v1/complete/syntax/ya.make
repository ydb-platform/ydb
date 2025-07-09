LIBRARY()

SRCS(
    ansi.cpp
    format.cpp
    grammar.cpp
)

PEERDIR(
    yql/essentials/core/issue
    yql/essentials/parser/antlr_ast/gen/v1_ansi_antlr4
    yql/essentials/parser/antlr_ast/gen/v1_antlr4
    yql/essentials/parser/lexer_common
    yql/essentials/sql/settings
    yql/essentials/sql/v1/lexer
    yql/essentials/sql/v1/lexer/regex
    yql/essentials/sql/v1/reflect
    yql/essentials/sql/v1/complete/antlr4
    yql/essentials/sql/v1/complete/core
    yql/essentials/sql/v1/complete/text
    contrib/libs/re2
)

END()

RECURSE_FOR_TESTS(
    ut
)
