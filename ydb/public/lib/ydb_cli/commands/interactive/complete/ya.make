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
    yql/essentials/sql/settings
    yql/essentials/sql/v1/format
    ydb/public/lib/ydb_cli/commands/interactive/antlr_ast/gen/v1_ansi_antlr4
    ydb/public/lib/ydb_cli/commands/interactive/antlr_ast/gen/v1_antlr4
)

END()

RECURSE_FOR_TESTS(
    ut
)
