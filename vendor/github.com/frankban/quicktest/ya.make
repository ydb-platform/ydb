GO_LIBRARY()

LICENSE(MIT)

SRCS(
    checker.go
    checker_err.go
    comment.go
    doc.go
    error.go
    format.go
    iter.go
    mapiter.go
    patch.go
    patch_go1.14.go
    quicktest.go
    report.go
)

GO_TEST_SRCS(export_test.go)

GO_XTEST_SRCS(
    checker_err_test.go
    checker_test.go
    cleanup_test.go
    comment_test.go
    error_test.go
    format_test.go
    patch_go1.14_test.go
    patch_test.go
    quicktest_test.go
    race_test.go
    report_test.go
)

END()

RECURSE(gotest)
