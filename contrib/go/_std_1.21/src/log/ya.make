GO_LIBRARY()

SRCS(
    log.go
)

GO_TEST_SRCS(log_test.go)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    internal
    slog
    syslog
)
