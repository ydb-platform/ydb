GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(any.pb.go)

GO_XTEST_SRCS(any_test.go)

END()

RECURSE(gotest)
