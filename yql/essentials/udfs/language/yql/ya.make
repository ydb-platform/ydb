YQL_UDF(yql_language_udf)

YQL_ABI_VERSION(
    2
    39
    0
)

SUBSCRIBER(g:yql)

SRCS(
    yql_language_udf.cpp
)

PEERDIR(
    yql/essentials/sql
    yql/essentials/sql/v1
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
    yql/essentials/sql/pg_dummy
    yql/essentials/sql/v1/format
    library/cpp/protobuf/util
)

END()

RECURSE_FOR_TESTS(
    test
)
