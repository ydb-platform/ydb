GO_LIBRARY()

SRCS(
    dump.go
    httputil.go
    persist.go
    reverseproxy.go
)

GO_TEST_SRCS(
    dump_test.go
    reverseproxy_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
)
