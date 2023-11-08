GO_LIBRARY()

SRCS(
    client.go
    doc.go
    env.go
    options.go
    responses.go
)

GO_XTEST_SRCS(client_test.go)

END()

RECURSE(gotest)
