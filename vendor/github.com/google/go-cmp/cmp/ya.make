GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    compare.go
    export_unsafe.go
    options.go
    path.go
    report.go
    report_compare.go
    report_references.go
    report_reflect.go
    report_slices.go
    report_text.go
    report_value.go
)

GO_TEST_SRCS(options_test.go)

GO_XTEST_SRCS(
    compare_test.go
    example_reporter_test.go
    example_test.go
)

END()

RECURSE(
    cmpopts
    #gotest
    internal
)
