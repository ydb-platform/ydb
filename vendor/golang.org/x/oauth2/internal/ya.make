GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    doc.go
    oauth2.go
    token.go
    transport.go
)

GO_TEST_SRCS(token_test.go)

END()

RECURSE(
    gotest
)
