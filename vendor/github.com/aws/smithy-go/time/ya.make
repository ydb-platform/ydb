GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    time.go
)

GO_TEST_SRCS(time_test.go)

END()

RECURSE(
    gotest
)
