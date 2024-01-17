GO_LIBRARY()

SRCS(
    position.go
    serialize.go
    token.go
)

GO_TEST_SRCS(
    position_bench_test.go
    position_test.go
    serialize_test.go
    token_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
)
