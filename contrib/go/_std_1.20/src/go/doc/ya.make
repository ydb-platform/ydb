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

END()

RECURSE(
    comment
)
