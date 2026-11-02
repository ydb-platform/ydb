YQL_LIBRARY()

SRCS(generated_column_expr.cpp)

PEERDIR(
    yql/essentials/core
    yql/essentials/core/expr_nodes
    yql/essentials/sql
    yql/essentials/sql/settings
    yql/essentials/sql/v1/translation
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
)

END()
