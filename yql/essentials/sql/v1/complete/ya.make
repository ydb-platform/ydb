LIBRARY()

SRCS(
    sql_complete.cpp
)

PEERDIR(
    yql/essentials/sql/v1/lexer
    yql/essentials/sql/v1/complete/antlr4
    yql/essentials/sql/v1/complete/name/service
    yql/essentials/sql/v1/complete/syntax
    yql/essentials/sql/v1/complete/text

    # TODO(YQL-19747): add it to YDB CLI PEERDIR
    yql/essentials/sql/v1/complete/name/service/static
)

END()

RECURSE(
    antlr4
    core
    name
    syntax
    text
)

RECURSE_FOR_TESTS(
    ut
)
