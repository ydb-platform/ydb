GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(admin.go)

GO_XTEST_SRCS(admin_test.go)

END()

RECURSE(
    gotest
    test
)
