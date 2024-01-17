GO_LIBRARY()

SRCS(
    pem.go
)

GO_TEST_SRCS(pem_test.go)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
)
