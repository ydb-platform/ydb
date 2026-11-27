LIBRARY()

SRCS(
    symbols.cpp
)

PEERDIR(
    yql/essentials/public/langver
    yql/essentials/ast
    yql/essentials/public/issue
    yql/essentials/parser/proto_ast/gen/v1_proto_split_antlr4
    yql/essentials/sql/settings
    yql/essentials/sql/v1/proto_parser
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
)

END()
