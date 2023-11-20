GO_LIBRARY()

SRCS(
    scanner.go
)

GO_TEST_SRCS(scanner_test.go)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
)
