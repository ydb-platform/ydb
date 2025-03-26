GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.32.0)

SRCS(
    scrypt.go
)

GO_TEST_SRCS(scrypt_test.go)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
)
