GO_LIBRARY()

LICENSE(BSD-2-Clause)

SRCS(
    benchmark.go
    check.go
    checkers.go
    helpers.go
    printer.go
    reporter.go
    run.go
)

GO_TEST_SRCS(export_test.go)

GO_XTEST_SRCS(
    benchmark_test.go
    bootstrap_test.go
    check_test.go
    checkers_test.go
    fixture_test.go
    foundation_test.go
    helpers_test.go
    integration_test.go
    printer_test.go
    reporter_test.go
    run_test.go
)

END()

RECURSE(gotest)
