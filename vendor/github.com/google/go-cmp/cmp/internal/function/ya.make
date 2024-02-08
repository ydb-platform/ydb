GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    func.go
)

GO_TEST_SRCS(func_test.go)

END()

RECURSE(
    gotest
)
