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
)

END()

RECURSE_FOR_TESTS(
    ut
)
