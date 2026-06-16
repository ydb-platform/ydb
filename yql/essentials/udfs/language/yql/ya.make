IF (NOT EXPORT_CMAKE OR NOT OPENSOURCE OR OPENSOURCE_PROJECT != "yt")

YQL_UDF(yql_language_udf)

YQL_ABI_VERSION(
    2
    39
    0
)

SUBSCRIBER(g:yql)

SRCS(
    sql2yql.cpp
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
    yql/essentials/sql/v1/reflect
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/proto
    yql/essentials/providers/common/gateways_utils
    library/cpp/protobuf/util
)

END()

ENDIF()

RECURSE_FOR_TESTS(
    test
)
