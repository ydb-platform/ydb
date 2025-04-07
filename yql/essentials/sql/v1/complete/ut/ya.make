UNITTEST_FOR(yql/essentials/sql/v1/complete)

SRCS(
    sql_complete_ut.cpp
)

PEERDIR(
    yql/essentials/sql/v1/complete/name/fallback
    yql/essentials/sql/v1/lexer/antlr4_pure
    yql/essentials/sql/v1/lexer/antlr4_pure_ansi
)

END()
