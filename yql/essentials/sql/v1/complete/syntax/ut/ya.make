UNITTEST_FOR(yql/essentials/sql/v1/complete/syntax)

SRCS(
    grammar_ut.cpp
    cursor_token_context_ut.cpp
)

PEERDIR(
    yql/essentials/sql/v1/lexer/antlr4_pure
)

END()
