GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(rls.go)

GO_TEST_SRCS(rls_test.go)

END()

RECURSE(gotest)
