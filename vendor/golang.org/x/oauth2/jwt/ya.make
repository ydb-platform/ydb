GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    jwt.go
)

GO_TEST_SRCS(jwt_test.go)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
)
