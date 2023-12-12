GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    scheme.go
)

GO_TEST_SRCS(scheme_test.go)

END()

RECURSE(
    gotest
)
