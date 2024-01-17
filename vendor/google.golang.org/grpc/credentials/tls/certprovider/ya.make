GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    distributor.go
    provider.go
    store.go
)

GO_TEST_SRCS(
    distributor_test.go
    store_test.go
)

END()

RECURSE(
    # gotest
    pemfile
)
