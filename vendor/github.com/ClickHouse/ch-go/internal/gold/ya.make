GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(gold.go)

GO_XTEST_SRCS(gold_test.go)

END()

RECURSE(gotest)
