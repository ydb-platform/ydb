GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    server.go
    server_options.go
    xds.go
)

GO_TEST_SRCS(server_test.go)

END()

RECURSE(
    bootstrap
    csds
    googledirectpath
    gotest
    internal
)
