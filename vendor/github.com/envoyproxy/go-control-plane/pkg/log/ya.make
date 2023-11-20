GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    default.go
    log.go
)

GO_TEST_SRCS(log_test.go)

END()

RECURSE(gotest)
