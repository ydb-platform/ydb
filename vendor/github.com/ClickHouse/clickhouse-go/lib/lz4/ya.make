GO_LIBRARY()

LICENSE(MIT)

SRCS(
    doc.go
    reader.go
    writer.go
)

GO_TEST_SRCS(lz4_test.go)

END()

RECURSE(gotest)
