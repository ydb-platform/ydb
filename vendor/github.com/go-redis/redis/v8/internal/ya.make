GO_LIBRARY()

LICENSE(BSD-2-Clause)

SRCS(
    arg.go
    internal.go
    log.go
    once.go
    unsafe.go
    util.go
)

GO_TEST_SRCS(internal_test.go)

END()

RECURSE(
    gotest
    hashtag
    hscan
    pool
    proto
    rand
    util
)
