GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(bootstrap.go)

GO_TEST_SRCS(bootstrap_test.go)

END()

RECURSE(gotest)
