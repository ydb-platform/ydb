GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    clientauth.go
    sts_exchange.go
)

GO_TEST_SRCS(
    clientauth_test.go
    sts_exchange_test.go
)

END()

RECURSE(
    gotest
)
