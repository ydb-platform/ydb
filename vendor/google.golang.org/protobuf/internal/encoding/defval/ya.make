GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(default.go)

GO_XTEST_SRCS(default_test.go)

END()

RECURSE(gotest)
