GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    api_client.go
    api_op_CreateToken.go
    api_op_RegisterClient.go
    api_op_StartDeviceAuthorization.go
    deserializers.go
    doc.go
    endpoints.go
    go_module_metadata.go
    serializers.go
    validators.go
)

GO_TEST_SRCS(
    api_client_test.go
    endpoints_test.go
    protocol_test.go
)

END()

RECURSE(
    gotest
    internal
    types
)
