PROGRAM()

PEERDIR(
    library/cpp/getopt
    contrib/libs/protobuf
    yql/essentials/sql/v1/format
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
)

SRCS(
    sql_formatter.cpp
)

END()
