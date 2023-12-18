GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    safe.go
)

GO_TEST_SRCS(safe_test.go)

END()

RECURSE(
    gotest
)
