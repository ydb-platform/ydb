GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(protogen.go)

GO_TEST_SRCS(protogen_test.go)

END()

RECURSE(gotest)
