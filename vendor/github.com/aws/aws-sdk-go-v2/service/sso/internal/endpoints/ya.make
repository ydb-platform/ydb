GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    endpoints.go
)

GO_TEST_SRCS(endpoints_test.go)

END()

RECURSE(
    gotest
)
