GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    enum.go
    message.go
)

GO_XTEST_SRCS(prototest_test.go)

END()

RECURSE(gotest)
