GO_LIBRARY()

SRCS(
    doc.go
    interface.go
    mock.go
)

END()

RECURSE(
    rdbms
    s3
)
