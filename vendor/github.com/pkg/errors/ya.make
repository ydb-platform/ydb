GO_LIBRARY()

LICENSE(BSD-2-Clause)

SRCS(
    errors.go
    go113.go
    stack.go
)

GO_TEST_SRCS(
    bench_test.go
    errors_test.go
    format_test.go
    go113_test.go
    json_test.go
    stack_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(gotest)
