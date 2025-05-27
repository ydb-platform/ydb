GO_LIBRARY()

LICENSE(MIT)

VERSION(v1.5.0)

SRCS(
    collection.go
    constraints.go
    doc.go
    version.go
)

GO_TEST_SRCS(
    collection_test.go
    constraints_test.go
    version_test.go
)

GO_XTEST_SRCS(benchmark_test.go)

END()

RECURSE(
    gotest
)
