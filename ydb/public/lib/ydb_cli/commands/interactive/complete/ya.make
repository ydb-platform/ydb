LIBRARY()

SRCS(
    sql_antlr4.cpp
    sql_complete.cpp
    sql_syntax.cpp
    string_util.cpp
)

PEERDIR(
    contrib/libs/antlr4_cpp_runtime
    contrib/libs/antlr4-c3
    yql/essentials/sql/v1/format
)

END()

RECURSE_FOR_TESTS(
    ut
)
