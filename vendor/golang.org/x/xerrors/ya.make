GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    adaptor.go
    doc.go
    errors.go
    fmt.go
    format.go
    frame.go
    wrap.go
)

GO_TEST_SRCS(fmt_unexported_test.go)

GO_XTEST_SRCS(
    errors_test.go
    example_As_test.go
    example_FormatError_test.go
    example_test.go
    fmt_test.go
    stack_test.go
    wrap_113_test.go
    wrap_test.go
)

END()

RECURSE(
    gotest
    internal
)
