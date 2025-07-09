UNITTEST_FOR(yql/essentials/sql/v1/lexer/regex)

PEERDIR(
    yql/essentials/sql/v1/lexer
    yql/essentials/sql/v1/lexer/antlr4_pure_ansi
)

SRCS(
    lexer_ut.cpp
    regex_ut.cpp
)

END()