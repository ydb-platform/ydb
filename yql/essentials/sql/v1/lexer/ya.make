LIBRARY()

PEERDIR(
    yql/essentials/public/issue/protos
    yql/essentials/sql/settings
)

SRCS(
    lexer.cpp
)

SUPPRESSIONS(
    tsan.supp
)

END()

RECURSE(
    antlr4
    antlr4_ansi
    antlr4_pure
    antlr4_pure_ansi
    check
    regex
)

RECURSE_FOR_TESTS(
    ut
)

