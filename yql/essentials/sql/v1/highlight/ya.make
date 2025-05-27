LIBRARY()

SRCS(
    sql_highlight_json.cpp
    sql_highlight.cpp
    sql_highlighter.cpp
)

PEERDIR(
    yql/essentials/sql/v1/lexer/regex
    yql/essentials/sql/v1/reflect
)

RESOURCE(yql/essentials/sql/v1/highlight/ut/suite.json suite.json)

END()

RECURSE_FOR_TESTS(
    ut
)
