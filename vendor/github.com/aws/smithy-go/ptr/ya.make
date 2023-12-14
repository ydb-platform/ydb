GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    doc.go
    from_ptr.go
    to_ptr.go
)

GO_TEST_SRCS(to_ptr_test.go)

END()

RECURSE(
    gotest
)
