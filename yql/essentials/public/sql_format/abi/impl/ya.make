LIBRARY()

SRCS(
    sql_format.cpp
)

PEERDIR(
    yql/essentials/public/sql_format/abi/interface
    yql/essentials/sql/v1/format
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
)

END()
