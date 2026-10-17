G_BENCHMARK()

SRCS(
    benchmark.cpp
)

PEERDIR(
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/lexer
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
    yql/essentials/sql/v1/proto_parser
    yql/essentials/parser/proto_ast/collect_issues
    yql/essentials/parser/proto_ast/gen/v1_proto_split_antlr4
)

RESOURCE(
    yql/essentials/tests/sql/suites/select_yql/minimal.yql select-yql-minimal.yql
    yql/essentials/tests/sql/suites/select_yql_tpcds/q47.yql yql-tpcds-q47.yql
)

END()
