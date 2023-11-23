GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    cache.go
    delta.go
    linear.go
    mux.go
    resource.go
    resources.go
    simple.go
    snapshot.go
    status.go
)

GO_TEST_SRCS(
    linear_test.go
    status_test.go
)

GO_XTEST_SRCS(
    cache_test.go
    delta_test.go
    fixtures_test.go
    resource_test.go
    resources_test.go
    simple_test.go
    snapshot_test.go
)

END()

RECURSE(
    # gotest
)
