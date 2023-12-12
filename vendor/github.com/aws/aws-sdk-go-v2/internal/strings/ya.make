GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    strings.go
)

GO_TEST_SRCS(strings_test.go)

END()

RECURSE(
    gotest
)
