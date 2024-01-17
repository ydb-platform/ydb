GO_LIBRARY()

SRCS(
    comment.go
    gobuild.go
    nodes.go
    printer.go
)

GO_TEST_SRCS(
    performance_test.go
    printer_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
)
