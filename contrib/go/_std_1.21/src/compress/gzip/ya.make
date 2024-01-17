GO_LIBRARY()

SRCS(
    gunzip.go
    gzip.go
)

GO_TEST_SRCS(
    fuzz_test.go
    gunzip_test.go
    gzip_test.go
    issue14937_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
)
