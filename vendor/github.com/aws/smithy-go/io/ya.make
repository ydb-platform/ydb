GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    byte.go
    doc.go
    reader.go
    ringbuffer.go
)

GO_TEST_SRCS(ringbuffer_test.go)

END()

RECURSE(
    gotest
)
