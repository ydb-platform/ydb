GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.1.1)

SRCS(
    cryptorandomstringutils.go
    randomstringutils.go
    stringutils.go
    wordutils.go
)

GO_TEST_SRCS(
    cryptorandomstringutils_test.go
    randomstringutils_test.go
    stringutils_test.go
    wordutils_test.go
)

END()

RECURSE(
    gotest
)
