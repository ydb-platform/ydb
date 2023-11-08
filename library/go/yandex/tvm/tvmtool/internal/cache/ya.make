GO_LIBRARY()

SRCS(cache.go)

GO_XTEST_SRCS(cache_test.go)

END()

RECURSE_FOR_TESTS(gotest)
