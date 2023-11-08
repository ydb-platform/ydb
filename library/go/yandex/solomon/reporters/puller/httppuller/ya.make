GO_LIBRARY()

SRCS(
    handler.go
    logger.go
    spack.go
    tvm.go
)

GO_TEST_SRCS(handler_test.go)

GO_XTEST_SRCS(
    example_test.go
    tvm_test.go
)

END()

RECURSE(gotest)
