GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    catmsg.go
    codec.go
    varint.go
)

GO_TEST_SRCS(
    catmsg_test.go
    varint_test.go
)

END()

RECURSE(gotest)
