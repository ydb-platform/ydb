GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    middleware.go
    presign_middleware.go
    stream.go
    v4.go
)

GO_TEST_SRCS(
    middleware_test.go
    presign_middleware_test.go
    v4_test.go
)

GO_XTEST_SRCS(functional_test.go)

END()

RECURSE(
    gotest
)
