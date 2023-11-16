GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    compact.go
    print.go
    triegen.go
)

GO_XTEST_SRCS(
    data_test.go
    example_compact_test.go
    example_test.go
)

END()

RECURSE(gotest)
