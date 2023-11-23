GO_LIBRARY()

SRCS(
    base32.go
)

GO_TEST_SRCS(base32_test.go)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
)
