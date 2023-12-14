GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    doc.go
    rand.go
    uuid.go
)

GO_XTEST_SRCS(uuid_test.go)

END()

RECURSE(
    gotest
)
