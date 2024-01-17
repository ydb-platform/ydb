GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    credentials.go
    tls.go
)

GO_TEST_SRCS(credentials_test.go)

END()

RECURSE(
    alts
    google
    # gotest
    insecure
    local
    oauth
    sts
    tls
    xds
)
