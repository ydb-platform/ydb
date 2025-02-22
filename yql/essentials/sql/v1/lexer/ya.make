LIBRARY()

PEERDIR(
    yql/essentials/core/issue/protos
    yql/essentials/sql/settings
    yql/essentials/sql/v1/lexer/antlr3
    yql/essentials/sql/v1/lexer/antlr3_ansi
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
)

SRCS(
    lexer.cpp
)

SUPPRESSIONS(
    tsan.supp
)

END()

RECURSE(
    antlr3
    antlr3_ansi
    antlr4
    antlr4_ansi
)

RECURSE_FOR_TESTS(
    ut
)

