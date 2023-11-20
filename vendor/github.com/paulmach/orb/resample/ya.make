GO_LIBRARY()

LICENSE(MIT)

SRCS(line_string.go)

GO_TEST_SRCS(line_string_test.go)

GO_XTEST_SRCS(
    benchmarks_test.go
    example_test.go
)

END()

RECURSE(gotest)
