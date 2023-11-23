GO_LIBRARY()

LICENSE(MIT)

SRCS(sanitize.go)

GO_XTEST_SRCS(sanitize_test.go)

END()

RECURSE(gotest)
