GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    catalog.go
    doc.go
    format.go
    message.go
    print.go
)

GO_TEST_SRCS(
    catalog_test.go
    fmt_test.go
    message_test.go
)

GO_XTEST_SRCS(examples_test.go)

END()

RECURSE(
    catalog
    #gotest
    pipeline
)
