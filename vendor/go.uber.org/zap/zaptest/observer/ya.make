GO_LIBRARY()

LICENSE(MIT)

SRCS(
    logged_entry.go
    observer.go
)

GO_TEST_SRCS(logged_entry_test.go)

GO_XTEST_SRCS(observer_test.go)

END()

RECURSE(
    gotest
)
