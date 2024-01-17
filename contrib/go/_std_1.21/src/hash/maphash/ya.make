GO_LIBRARY()

SRCS(
    maphash.go
    maphash_runtime.go
)

GO_TEST_SRCS(
    maphash_test.go
    smhasher_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
)
