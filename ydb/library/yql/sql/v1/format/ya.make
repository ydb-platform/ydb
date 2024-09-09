LIBRARY()

SET(GRAMMAR_STRING_CORE_SINGLE "\"~(QUOTE_SINGLE | BACKSLASH) | (BACKSLASH .)\"")
SET(GRAMMAR_STRING_CORE_DOUBLE "\"~(QUOTE_DOUBLE | BACKSLASH) | (BACKSLASH .)\"")
SET(GRAMMAR_MULTILINE_COMMENT_CORE       "\".\"")

SRCS(
    sql_format.cpp
)

RESOURCE(DONT_PARSE ../SQLv1.g.in SQLv1.g.in)
RESOURCE(DONT_PARSE ../SQLv1Antlr4.g.in SQLv1Antlr4.g.in)

PEERDIR(
    ydb/library/yql/parser/lexer_common
    ydb/library/yql/sql/settings
    ydb/library/yql/sql/v1/lexer
    ydb/library/yql/sql/v1/proto_parser
    ydb/library/yql/core/sql_types
    library/cpp/protobuf/util
    library/cpp/resource
)

END()

RECURSE_FOR_TESTS(
    ut
    ut_antlr4
)
