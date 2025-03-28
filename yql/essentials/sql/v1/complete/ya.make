LIBRARY()

SRCS(
    sql_antlr4.cpp
    sql_complete.cpp
    sql_context.cpp
    sql_syntax.cpp
    string_util.cpp
)

PEERDIR(
    contrib/libs/antlr4_cpp_runtime
    contrib/libs/antlr4-c3
    yql/essentials/core/issue
    yql/essentials/sql/settings
    yql/essentials/sql/v1/format
    yql/essentials/sql/v1/lexer

    # FIXME(YQL-19747): unwanted dependency on a lexer implementation
    yql/essentials/sql/v1/lexer/antlr4_pure
    yql/essentials/sql/v1/lexer/antlr4_pure_ansi

    yql/essentials/parser/antlr_ast/gen/v1_ansi_antlr4
    yql/essentials/parser/antlr_ast/gen/v1_antlr4

    yql/essentials/sql/v1/complete/name
    yql/essentials/sql/v1/complete/name/static
)

END()

RECURSE_FOR_TESTS(
    ut
)
