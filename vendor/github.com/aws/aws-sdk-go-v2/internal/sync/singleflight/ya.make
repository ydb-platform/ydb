GO_LIBRARY()

LICENSE(Apache-2.0)

GO_SKIP_TESTS(
    TestPanicDoChan
    TestPanicDoSharedByDoChan
)

SRCS(
    docs.go
    singleflight.go
)

GO_TEST_SRCS(singleflight_test.go)

END()

RECURSE(
    gotest
)
