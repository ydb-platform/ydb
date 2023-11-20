GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(constraints.go)

GO_TEST_SRCS(constraints_test.go)

END()

RECURSE(gotest)
