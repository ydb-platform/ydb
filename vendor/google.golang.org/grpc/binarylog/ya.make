GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(sink.go)

GO_XTEST_SRCS(binarylog_end2end_test.go)

END()

RECURSE(
    gotest
    grpc_binarylog_v1
)
