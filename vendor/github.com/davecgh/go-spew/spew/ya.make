GO_LIBRARY()

LICENSE(ISC)

SRCS(
    bypass.go
    common.go
    config.go
    doc.go
    dump.go
    format.go
    spew.go
)

GO_TEST_SRCS(
    internal_test.go
    internalunsafe_test.go
)

GO_XTEST_SRCS(
    common_test.go
    dump_test.go
    dumpnocgo_test.go
    example_test.go
    format_test.go
    spew_test.go
)

END()

RECURSE(gotest)
