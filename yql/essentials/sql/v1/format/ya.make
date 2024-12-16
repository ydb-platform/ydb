LIBRARY()


SRCS(
    sql_format.cpp
)

RESOURCE(DONT_PARSE yql/essentials/sql/v1/SQLv1.g.in SQLv1.g.in)
RESOURCE(DONT_PARSE yql/essentials/sql/v1/SQLv1Antlr4.g.in SQLv1Antlr4.g.in)

PEERDIR(
    yql/essentials/parser/lexer_common
    yql/essentials/sql/settings
    yql/essentials/sql/v1/lexer
    yql/essentials/sql/v1/proto_parser
    yql/essentials/core/sql_types
    library/cpp/protobuf/util
    library/cpp/resource
)

END()

RECURSE_FOR_TESTS(
    ut
    ut_antlr4
)
