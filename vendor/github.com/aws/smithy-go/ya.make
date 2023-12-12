GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    doc.go
    document.go
    errors.go
    go_module_metadata.go
    properties.go
    validation.go
)

GO_TEST_SRCS(properties_test.go)

END()

RECURSE(
    auth
    context
    document
    encoding
    endpoints
    gotest
    internal
    io
    logging
    middleware
    ptr
    rand
    sync
    testing
    time
    transport
    waiter
)
