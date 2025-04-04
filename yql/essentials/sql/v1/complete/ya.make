LIBRARY()

SRCS(
    sql_complete.cpp
)

PEERDIR(
    yql/essentials/sql/v1/lexer

    # FIXME(YQL-19747): unwanted dependency on a lexer implementation
    yql/essentials/sql/v1/lexer/antlr4_pure
    yql/essentials/sql/v1/lexer/antlr4_pure_ansi

    yql/essentials/sql/v1/complete/antlr4
    yql/essentials/sql/v1/complete/name
    yql/essentials/sql/v1/complete/name/static
    yql/essentials/sql/v1/complete/syntax
    yql/essentials/sql/v1/complete/text
)

END()

RECURSE_FOR_TESTS(
    ut
)
