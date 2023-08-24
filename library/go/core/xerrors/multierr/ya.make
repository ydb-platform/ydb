GO_LIBRARY()

SRCS(
    error.go
)

GO_TEST_SRCS(
    error_test.go
)

END()

RECURSE(
    gotest
)
