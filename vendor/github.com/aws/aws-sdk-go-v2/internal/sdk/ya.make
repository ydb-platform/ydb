GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    interfaces.go
    time.go
)

GO_TEST_SRCS(time_test.go)

END()

RECURSE(
    gotest
)
