GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    handlers.go
    stats.go
)

GO_XTEST_SRCS(stats_test.go)

END()

RECURSE(
    gotest
    # yo
)
