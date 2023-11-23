GO_LIBRARY()

SRCS(
    format.go
    internal.go
)

GO_TEST_SRCS(format_test.go)

GO_XTEST_SRCS(
    benchmark_test.go
    example_test.go
)

END()

RECURSE(
)
