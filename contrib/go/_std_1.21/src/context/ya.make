GO_LIBRARY()

SRCS(
    context.go
)

GO_TEST_SRCS(context_test.go)

GO_XTEST_SRCS(
    afterfunc_test.go
    benchmark_test.go
    example_test.go
    net_test.go
    x_test.go
)

END()

RECURSE(
)
