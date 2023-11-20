GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    discard.go
    logr.go
)

GO_TEST_SRCS(
    discard_test.go
    logr_test.go
)

GO_XTEST_SRCS(
    example_marshaler_secret_test.go
    example_marshaler_test.go
    example_test.go
)

END()

RECURSE(
    benchmark
    examples
    funcr
    # gotest
    testing
    testr
)
