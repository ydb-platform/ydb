GO_LIBRARY()

LICENSE(MIT)

SRCS(
    pool.go
)

GO_XTEST_SRCS(pool_test.go)

END()

RECURSE(
    gotest
)
