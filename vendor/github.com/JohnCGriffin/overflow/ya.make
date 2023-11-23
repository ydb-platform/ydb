GO_LIBRARY()

LICENSE(MIT)

SRCS(
    overflow.go
    overflow_impl.go
)

GO_TEST_SRCS(overflow_test.go)

END()

RECURSE(gotest)
