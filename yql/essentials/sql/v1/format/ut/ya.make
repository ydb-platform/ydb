UNITTEST_FOR(yql/essentials/sql/v1/format)

SRCS(
    sql_format_ut.cpp
)

PEERDIR(
    yql/essentials/sql/v1/lexer/antlr3
    yql/essentials/sql/v1/lexer/antlr3_ansi
    yql/essentials/sql/v1/proto_parser/antlr3
    yql/essentials/sql/v1/proto_parser/antlr3_ansi
)

END()
