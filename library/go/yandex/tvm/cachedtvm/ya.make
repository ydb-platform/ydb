GO_LIBRARY()

SRCS(
    cache.go
    client.go
    opts.go
)

GO_XTEST_SRCS(
    client_example_test.go
    client_test.go
)

END()

RECURSE(gotest)
