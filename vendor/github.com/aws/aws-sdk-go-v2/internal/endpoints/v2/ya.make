GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    endpoints.go
    go_module_metadata.go
)

GO_TEST_SRCS(endpoints_test.go)

GO_XTEST_SRCS(package_test.go)

END()

RECURSE(
    gotest
)
