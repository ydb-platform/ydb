GO_LIBRARY()

LICENSE(Apache-2.0)

GO_SKIP_TESTS(
    Test
    TestSDKAllowsRPCRequestWithEmptyPrincipalsOnMTLSAuthenticatedConnection
    SDKAllowsRPCRequestWithEmptyPrincipalsOnTLSAuthenticatedConnection
)

SRCS(
    grpc_authz_server_interceptors.go
    rbac_translator.go
)

GO_TEST_SRCS(rbac_translator_test.go)

GO_XTEST_SRCS(
    grpc_authz_end2end_test.go
    grpc_authz_server_interceptors_test.go
)

END()

RECURSE(
    audit
    gotest
)
