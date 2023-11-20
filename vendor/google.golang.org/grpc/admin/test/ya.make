GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(utils.go)

GO_XTEST_SRCS(admin_test.go)

END()

RECURSE(gotest)
