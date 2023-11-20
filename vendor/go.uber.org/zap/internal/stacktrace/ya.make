GO_LIBRARY()

LICENSE(MIT)

SRCS(
    stack.go
)

GO_TEST_SRCS(stack_test.go)

END()

RECURSE(
    gotest
)
