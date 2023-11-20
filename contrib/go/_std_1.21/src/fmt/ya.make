GO_LIBRARY()

SRCS(
    doc.go
    errors.go
    format.go
    print.go
    scan.go
)

GO_TEST_SRCS(export_test.go)

GO_XTEST_SRCS(
    errors_test.go
    example_test.go
    fmt_test.go
    gostringer_example_test.go
    scan_test.go
    state_test.go
    stringer_example_test.go
    stringer_test.go
)

END()

RECURSE(
)
