GO_LIBRARY()

SRCS(
    attr.go
    doc.go
    handler.go
    json_handler.go
    level.go
    logger.go
    record.go
    text_handler.go
    value.go
)

GO_TEST_SRCS(
    attr_test.go
    handler_test.go
    json_handler_test.go
    level_test.go
    logger_test.go
    record_test.go
    text_handler_test.go
    value_access_benchmark_test.go
    value_test.go
)

GO_XTEST_SRCS(
    example_custom_levels_test.go
    example_level_handler_test.go
    example_logvaluer_group_test.go
    example_logvaluer_secret_test.go
    example_test.go
    example_wrap_test.go
    slogtest_test.go
)

END()

RECURSE(
    internal
)
