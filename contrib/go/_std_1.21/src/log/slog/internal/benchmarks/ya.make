GO_LIBRARY()

SRCS(
    benchmarks.go
    handlers.go
)

GO_TEST_SRCS(
    benchmarks_test.go
    handlers_test.go
)

END()

RECURSE(
)
