UNITTEST_FOR(yql/essentials/sql/v1/format)

SRCS(
    sql_format_ut_antlr4.cpp
)

PEERDIR(
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
)


END()
