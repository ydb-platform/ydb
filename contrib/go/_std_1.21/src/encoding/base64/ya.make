GO_LIBRARY()

SRCS(
    base64.go
)

GO_TEST_SRCS(base64_test.go)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
)
