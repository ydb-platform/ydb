UNITTEST_FOR(yql/essentials/sql/v1/format/check)

SRCS(
    check_format_ut.cpp
)

PEERDIR(
    yql/essentials/sql
    yql/essentials/sql/pg_dummy
    yql/essentials/sql/v1
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/utils/string
)


END()
