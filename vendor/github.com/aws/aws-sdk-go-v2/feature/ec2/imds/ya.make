GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    api_client.go
    api_op_GetDynamicData.go
    api_op_GetIAMInfo.go
    api_op_GetInstanceIdentityDocument.go
    api_op_GetMetadata.go
    api_op_GetRegion.go
    api_op_GetToken.go
    api_op_GetUserData.go
    doc.go
    go_module_metadata.go
    request_middleware.go
    token_provider.go
)

GO_TEST_SRCS(
    api_client_test.go
    api_op_GetDynamicData_test.go
    api_op_GetIAMInfo_test.go
    api_op_GetInstanceIdentityDocument_test.go
    api_op_GetMetadata_test.go
    api_op_GetRegion_test.go
    api_op_GetToken_test.go
    api_op_GetUserData_test.go
    request_middleware_test.go
    shared_test.go
)

END()

RECURSE(
    gotest
    internal
)
