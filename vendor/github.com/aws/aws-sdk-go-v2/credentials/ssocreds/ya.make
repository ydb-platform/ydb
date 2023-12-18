GO_LIBRARY()

LICENSE(Apache-2.0)

GO_SKIP_TESTS(TestProvider)

SRCS(
    doc.go
    sso_cached_token.go
    sso_credentials_provider.go
    sso_token_provider.go
)

GO_TEST_SRCS(
    # sso_cached_token_test.go
    sso_credentials_provider_test.go
    # sso_token_provider_test.go
)

END()

RECURSE(
    gotest
)
