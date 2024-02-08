GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(proto.go)

GO_TEST_SRCS(
    proto_benchmark_test.go
    proto_test.go
)

END()

RECURSE(gotest)
