GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    assume_role_provider.go
    web_identity_provider.go
)

GO_XTEST_SRCS(
    assume_role_provider_test.go
    web_identity_provider_test.go
)

END()

RECURSE(
    gotest
)
