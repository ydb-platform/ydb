GO_LIBRARY()

LICENSE(BSD-2-Clause)

SRCS(
    reader.go
    scan.go
    writer.go
)

GO_XTEST_SRCS(
    proto_test.go
    reader_test.go
    scan_test.go
    writer_test.go
)

END()

RECURSE(gotest)
