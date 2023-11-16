GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(format.go)

GO_XTEST_SRCS(format_test.go)

END()

RECURSE(gotest)
