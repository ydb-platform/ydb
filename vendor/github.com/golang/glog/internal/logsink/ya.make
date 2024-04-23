GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    logsink.go
    logsink_fatal.go
)

GO_XTEST_SRCS(logsink_test.go)

END()

RECURSE(
    gotest
)
