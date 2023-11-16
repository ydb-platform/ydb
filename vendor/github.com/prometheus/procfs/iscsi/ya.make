GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    get.go
    iscsi.go
)

GO_XTEST_SRCS(get_test.go)

END()

RECURSE(
    # gotest
)
