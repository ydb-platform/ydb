GO_LIBRARY()

LICENSE(MIT)

SRCS(
    diff.go
    formatter.go
    pretty.go
    zero.go
)

GO_TEST_SRCS(
    diff_test.go
    formatter_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(gotest)
