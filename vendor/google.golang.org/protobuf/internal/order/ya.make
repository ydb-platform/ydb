GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    order.go
    range.go
)

GO_TEST_SRCS(order_test.go)

END()

RECURSE(gotest)
