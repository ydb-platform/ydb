GO_LIBRARY()

LICENSE(MIT)

SRCS(stream.go)

GO_TEST_SRCS(
    bench_test.go
    stream_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(gotest)
