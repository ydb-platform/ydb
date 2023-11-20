GO_LIBRARY()

LICENSE(MIT)

SRCS(
    alt_exit.go
    buffer_pool.go
    doc.go
    entry.go
    exported.go
    formatter.go
    hooks.go
    json_formatter.go
    logger.go
    logrus.go
    text_formatter.go
    writer.go
)

GO_TEST_SRCS(
    alt_exit_test.go
    entry_test.go
    formatter_bench_test.go
    json_formatter_test.go
    logger_bench_test.go
    logger_test.go
    text_formatter_test.go
)

GO_XTEST_SRCS(
    example_basic_test.go
    example_custom_caller_test.go
    example_default_field_value_test.go
    example_function_test.go
    example_global_hook_test.go
    hook_test.go
    level_test.go
    logrus_test.go
    writer_test.go
)

IF (OS_LINUX)
    SRCS(
        terminal_check_notappengine.go
        terminal_check_unix.go
    )

    GO_XTEST_SRCS(example_hook_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        terminal_check_bsd.go
        terminal_check_notappengine.go
    )

    GO_XTEST_SRCS(example_hook_test.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(terminal_check_windows.go)
ENDIF()

END()

RECURSE(
    # gotest
    hooks
    internal
)

IF (OS_LINUX)
    RECURSE(hooks/syslog)
ENDIF()

IF (OS_DARWIN)
    RECURSE(hooks/syslog)
ENDIF()
