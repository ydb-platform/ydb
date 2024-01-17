GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    grpctest.go
    tlogger.go
)

GO_TEST_SRCS(
    grpctest_test.go
    tlogger_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(gotest)
