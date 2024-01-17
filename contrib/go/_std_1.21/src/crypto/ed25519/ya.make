GO_LIBRARY()

SRCS(
    ed25519.go
)

GO_TEST_SRCS(ed25519_test.go)

GO_XTEST_SRCS(ed25519vectors_test.go)

END()

RECURSE(
)
