UNITTEST_FOR(yql/essentials/sql/v1/proto_parser)

SRCS(
    range_ut.cpp
    proto_parser_ut.cpp
)

PEERDIR(
    yql/essentials/sql/v1/proto_parser/check
    yql/essentials/utils/string
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/lexer
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
)

END()
