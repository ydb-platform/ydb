LIBRARY()

SRCS(
    yql_highlighter.cpp
    yql_position.cpp
)

PEERDIR(
    contrib/restricted/patched/replxx
    yql/essentials/sql/v1/lexer
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/settings
)

END()

RECURSE_FOR_TESTS(
    ut
)
