GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    doc.go
    encoding.go
)

END()

RECURSE(
    httpbinding
    json
    xml
)
