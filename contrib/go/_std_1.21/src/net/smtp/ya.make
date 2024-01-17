GO_LIBRARY()

SRCS(
    auth.go
    smtp.go
)

GO_TEST_SRCS(smtp_test.go)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
)
