GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(struct.pb.go)

GO_XTEST_SRCS(struct_test.go)

END()

RECURSE(gotest)
