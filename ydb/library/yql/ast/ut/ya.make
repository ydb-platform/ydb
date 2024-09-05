UNITTEST_FOR(ydb/library/yql/ast)

FORK_SUBTESTS()

SRCS(
    yql_ast_ut.cpp
    yql_expr_check_args_ut.cpp
    yql_expr_builder_ut.cpp
    yql_expr_ut.cpp
    yql_type_string_ut.cpp
    yql_constraint_ut.cpp
)

PEERDIR(
    library/cpp/yson/node
)

END()
