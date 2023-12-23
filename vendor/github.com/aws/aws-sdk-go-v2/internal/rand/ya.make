GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    rand.go
)

GO_TEST_SRCS(rand_test.go)

END()

RECURSE(
    gotest
)
