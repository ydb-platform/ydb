GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    xxhash.go
    xxhash_go17.go
    xxhash_unsafe.go
)

GO_XTEST_SRCS(xxhash_test.go)

END()

RECURSE(gotest)
