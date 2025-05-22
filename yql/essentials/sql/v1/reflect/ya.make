LIBRARY()

SRCS(
    sql_reflect.cpp
)

RESOURCE(DONT_PARSE yql/essentials/sql/v1/SQLv1Antlr4.g.in SQLv1Antlr4.g.in)

END()

RECURSE_FOR_TESTS(
    ut
)
