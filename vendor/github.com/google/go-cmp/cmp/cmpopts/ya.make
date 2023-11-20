GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    equate.go
    ignore.go
    sort.go
    struct_filter.go
    xform.go
)

GO_TEST_SRCS(util_test.go)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(gotest)
