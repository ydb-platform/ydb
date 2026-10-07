LIBRARY()

SRCS(
    cursor_token_context.cpp
    local.cpp
    parser_call_stack.cpp
)

ADDINCL(
    yql/essentials/sql/v1/ide/completion
)

PEERDIR(
    yql/essentials/sql/v1/ide/completion/syntax
    yql/essentials/sql/v1/ide/pure_ast
)

END()

RECURSE_FOR_TESTS(
    ut
)
