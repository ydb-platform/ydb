GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(pack.go)

GO_TEST_SRCS(pack_test.go)

END()

RECURSE(gotest)
