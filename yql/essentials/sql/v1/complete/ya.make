LIBRARY()

SRCS(
    sql_complete.cpp
)

PEERDIR(
    yql/essentials/sql/v1/lexer
    yql/essentials/sql/v1/complete/antlr4
    yql/essentials/sql/v1/complete/name
    yql/essentials/sql/v1/complete/name/static
    yql/essentials/sql/v1/complete/syntax
    yql/essentials/sql/v1/complete/text
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
