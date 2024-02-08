GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(serverreflection.go)

GO_TEST_SRCS(serverreflection_test.go)

END()

RECURSE(
    gotest
    grpc_reflection_v1
    grpc_reflection_v1alpha
    grpc_testing
    grpc_testing_not_regenerate
    # yo
)
