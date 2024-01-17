GO_LIBRARY()

SRCS(
    trace.go
)

GO_TEST_SRCS(trace_test.go)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
)
