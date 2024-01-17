GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    credentials.go
    spiffe.go
    syscallconn.go
    util.go
)

GO_TEST_SRCS(
    spiffe_test.go
    syscallconn_test.go
    util_test.go
)

END()

RECURSE(
    gotest
    xds
)
