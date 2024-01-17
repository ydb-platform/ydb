GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    binarylog.go
    binarylog_testutil.go
    env_config.go
    method_logger.go
    sink.go
)

GO_TEST_SRCS(
    binarylog_test.go
    env_config_test.go
    method_logger_test.go
    regexp_test.go
)

END()

RECURSE(
    # gotest
)
