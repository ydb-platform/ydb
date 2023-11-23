GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    googlec2p.go
    utils.go
)

GO_TEST_SRCS(googlec2p_test.go)

END()

RECURSE(gotest)
