GO_LIBRARY()

LICENSE(MIT)

SRCS(
    gen_stack.go
    stack.go
)

GO_TEST_SRCS(gen_stack_test.go)

END()

RECURSE(
    gotest
)
