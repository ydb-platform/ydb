GO_LIBRARY()

SRCS(
    jar.go
    punycode.go
)

GO_TEST_SRCS(
    jar_test.go
    punycode_test.go
)

GO_XTEST_SRCS(
    dummy_publicsuffix_test.go
    example_test.go
)

END()

RECURSE(
)
