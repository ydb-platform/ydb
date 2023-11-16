GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    name.go
    pointer_unsafe.go
    sort.go
)

GO_TEST_SRCS(name_test.go)

GO_XTEST_SRCS(
    # sort_test.go
)

END()

RECURSE(gotest)
