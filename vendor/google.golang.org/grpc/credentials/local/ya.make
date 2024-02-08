GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(local.go)

GO_TEST_SRCS(local_test.go)

END()

RECURSE(gotest)
