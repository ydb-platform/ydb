GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(duration.pb.go)

GO_XTEST_SRCS(duration_test.go)

END()

RECURSE(gotest)
