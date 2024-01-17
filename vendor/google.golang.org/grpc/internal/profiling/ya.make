GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    goid_regular.go
    profiling.go
)

GO_TEST_SRCS(profiling_test.go)

END()

RECURSE(
    buffer
    gotest
)
