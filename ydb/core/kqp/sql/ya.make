LIBRARY()

SRCS(
    translator.cpp
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/core/sql_types
    yql/essentials/parser/proto_ast/gen/v1_antlr4
    yql/essentials/parser/proto_ast/gen/v1_proto_split_antlr4
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
    yql/essentials/sql/v1/translation
)

END()

RECURSE_FOR_TESTS(
    ut
)
