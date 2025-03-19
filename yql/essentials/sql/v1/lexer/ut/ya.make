UNITTEST_FOR(yql/essentials/sql/v1/lexer)

PEERDIR(
    yql/essentials/core/issue
    yql/essentials/parser/lexer_common
    yql/essentials/sql/v1/lexer/antlr3
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_pure
)

SRCS(
    lexer_ut.cpp
)

END()
