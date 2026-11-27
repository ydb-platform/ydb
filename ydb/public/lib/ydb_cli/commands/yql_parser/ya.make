LIBRARY()

PEERDIR(
    ydb/public/sdk/cpp/src/client/types
    ydb/public/sdk/cpp/src/client/value
    yql/essentials/sql/settings
    yql/essentials/sql/v1/lexer
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
)

SRCS(
    yql_parser.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
