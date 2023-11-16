GO_LIBRARY()

LICENSE(MIT)

SRCS(tracelog.go)

GO_XTEST_SRCS(
    # tracelog_test.go
)

END()

RECURSE(gotest)
