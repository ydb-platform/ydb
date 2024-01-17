GO_LIBRARY()

SRCS(
    bufio.go
    scan.go
)

GO_TEST_SRCS(export_test.go)

GO_XTEST_SRCS(
    bufio_test.go
    example_test.go
    scan_test.go
)

END()

RECURSE(
)
