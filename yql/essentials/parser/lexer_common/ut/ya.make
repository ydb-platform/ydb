UNITTEST_FOR(yql/essentials/parser/lexer_common)

PEERDIR(
    yql/essentials/sql/v1/lexer
    yql/essentials/sql/v1/lexer/antlr4
)


SRCS(
    hints_ut.cpp
    parse_hints_ut.cpp
)

END()
