GO_LIBRARY()

LICENSE(Apache-2.0)

GO_SKIP_TESTS(
    TestProviderAltConstruct
    TestProviderExpectErrors
    TestProviderExpired
    TestProviderForceExpire
    TestProviderNotExpired
    TestProviderStatic
    TestProviderTimeout
    TestProviderWithLongSessionToken
)

SRCS(
    doc.go
    provider.go
)

GO_TEST_SRCS(provider_test.go)

END()

RECURSE(
    gotest
)
