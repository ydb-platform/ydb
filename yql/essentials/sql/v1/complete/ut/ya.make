UNITTEST_FOR(yql/essentials/sql/v1/complete)

SRCS(
    sql_complete_ut.cpp
)

PEERDIR(
    yql/essentials/sql/v1/lexer/antlr4_pure
    yql/essentials/sql/v1/lexer/antlr4_pure_ansi
    yql/essentials/sql/v1/complete/name/service/static
)

END()
