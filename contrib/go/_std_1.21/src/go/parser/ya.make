GO_LIBRARY()

SRCS(
    interface.go
    parser.go
    resolver.go
)

GO_TEST_SRCS(
    error_test.go
    parser_test.go
    performance_test.go
    resolver_test.go
    short_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
)
