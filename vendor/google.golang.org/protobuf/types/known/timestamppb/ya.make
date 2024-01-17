GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(timestamp.pb.go)

GO_XTEST_SRCS(timestamp_test.go)

END()

RECURSE(gotest)
