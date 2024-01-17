GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(wire.go)

GO_TEST_SRCS(wire_test.go)

END()

RECURSE(gotest)
