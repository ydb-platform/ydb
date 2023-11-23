GO_LIBRARY()

LICENSE(MIT)

SRCS(
    exit.go
)

GO_XTEST_SRCS(exit_test.go)

END()

RECURSE(
    gotest
)
