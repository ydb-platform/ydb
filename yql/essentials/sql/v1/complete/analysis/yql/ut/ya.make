UNITTEST_FOR(yql/essentials/sql/v1/complete/analysis/yql)

SRCS(
    yql_ut.cpp
)

PEERDIR(
    yql/essentials/providers/common/provider
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/settings
    yql/essentials/sql/v1
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
)

END()
