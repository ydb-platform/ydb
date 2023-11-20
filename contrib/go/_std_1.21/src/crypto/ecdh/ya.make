GO_LIBRARY()

SRCS(
    ecdh.go
    nist.go
    x25519.go
)

GO_XTEST_SRCS(ecdh_test.go)

END()

RECURSE(
)
