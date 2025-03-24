LIBRARY()

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_value
    ydb/public/sdk/cpp/client/ydb_types
    yql/essentials/parser/lexer_common
    yql/essentials/parser/proto_ast
    yql/essentials/sql/settings
    yql/essentials/sql/v1/lexer
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
)

SRCS(
    yql_parser.cpp
)

END() 