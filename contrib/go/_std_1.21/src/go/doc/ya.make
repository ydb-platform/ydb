GO_LIBRARY()

SRCS(
    comment.go
    doc.go
    example.go
    exports.go
    filter.go
    reader.go
    synopsis.go
)

GO_TEST_SRCS(
    comment_test.go
    doc_test.go
    example_internal_test.go
    synopsis_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    comment
)
