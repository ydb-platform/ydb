GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    doc.go
    go_module_metadata.go
    static_provider.go
)

GO_TEST_SRCS(static_provider_test.go)

END()

RECURSE(
    ec2rolecreds
    endpointcreds
    gotest
    processcreds
    ssocreds
    stscreds
)
