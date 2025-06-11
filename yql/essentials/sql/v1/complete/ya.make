LIBRARY()

SRCS(
    sql_complete.cpp
)

PEERDIR(
    yql/essentials/sql/v1/lexer
    yql/essentials/sql/v1/complete/antlr4
    yql/essentials/sql/v1/complete/name/service
    # TODO(YQL-19747): move to /analysis/local
    yql/essentials/sql/v1/complete/syntax
    yql/essentials/sql/v1/complete/analysis/global
    yql/essentials/sql/v1/complete/text
    # TODO(YQL-19747): split /name/service/ranking interface and implementation
    # TODO(YQL-19747): extract NameIndex
    yql/essentials/sql/v1/complete/name/service/ranking
    yql/essentials/sql/v1/complete/name/service/binding
)

END()

RECURSE(
    analysis
    antlr4
    core
    name
    syntax
    text
)

RECURSE_FOR_TESTS(
    ut
)
