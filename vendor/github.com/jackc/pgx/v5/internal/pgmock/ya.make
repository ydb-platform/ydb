GO_LIBRARY()

LICENSE(MIT)

SRCS(pgmock.go)

GO_XTEST_SRCS(pgmock_test.go)

END()

RECURSE(gotest)
