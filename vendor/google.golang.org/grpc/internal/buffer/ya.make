GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(unbounded.go)

GO_TEST_SRCS(unbounded_test.go)

END()

RECURSE(gotest)
