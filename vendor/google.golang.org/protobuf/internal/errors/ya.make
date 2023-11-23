GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    errors.go
    is_go113.go
)

GO_TEST_SRCS(errors_test.go)

END()

RECURSE(gotest)
