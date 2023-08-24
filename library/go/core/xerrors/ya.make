GO_LIBRARY()

SRCS(
    doc.go
    errorf.go
    forward.go
    mode.go
    new.go
    sentinel.go
    stacktrace.go
)

GO_TEST_SRCS(
    benchmark_test.go
    errorf_formatting_with_error_test.go
    errorf_formatting_with_std_error_test.go
    errorf_formatting_without_error_test.go
    errorf_multiple_errors_test.go
    new_formatting_test.go
    sentinel_test.go
    sentinel_with_stack_formatting_with_colon_test.go
    sentinel_with_stack_formatting_without_colon_test.go
    sentinel_wrap_formatting_with_colon_test.go
    sentinel_wrap_formatting_without_colon_test.go
    sentinel_wrap_new_formatting_test.go
)

END()

RECURSE(
    assertxerrors
    benchxerrors
    gotest
    internal
    multierr
)
