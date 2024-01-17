GO_LIBRARY()

SRCS(
    sort.go
)

GO_TEST_SRCS(export_test.go)

GO_XTEST_SRCS(sort_test.go)

END()

RECURSE(
)
