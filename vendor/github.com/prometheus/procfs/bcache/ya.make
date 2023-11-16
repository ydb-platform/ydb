GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    bcache.go
    get.go
)

GO_TEST_SRCS(get_test.go)

END()

RECURSE(
    # gotest
)
