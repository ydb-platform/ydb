GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    doc.go
    strings.go
    uri.go
)

GO_TEST_SRCS(
    strings_test.go
    uri_test.go
)

END()

RECURSE(
    gotest
)
