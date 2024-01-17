GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    externalaccountauthorizeduser.go
)

GO_TEST_SRCS(externalaccountauthorizeduser_test.go)

END()

RECURSE(
    gotest
)
