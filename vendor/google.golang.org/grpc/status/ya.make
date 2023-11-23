GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(status.go)

GO_TEST_SRCS(status_test.go)

GO_XTEST_SRCS(status_ext_test.go)

END()

RECURSE(
    # gotest
)
