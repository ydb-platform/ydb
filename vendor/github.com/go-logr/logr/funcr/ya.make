GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(funcr.go)

GO_TEST_SRCS(funcr_test.go)

GO_XTEST_SRCS(
    example_formatter_test.go
    example_test.go
)

END()

RECURSE(
    example
    gotest
)
