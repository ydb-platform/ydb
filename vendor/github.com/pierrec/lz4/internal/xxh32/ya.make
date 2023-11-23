GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(xxh32zero.go)

GO_XTEST_SRCS(xxh32zero_test.go)

END()

RECURSE(gotest)
