GO_LIBRARY()

SRCS(
    message.go
)

GO_TEST_SRCS(message_test.go)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
)
