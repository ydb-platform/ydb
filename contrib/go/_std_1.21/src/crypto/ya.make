GO_LIBRARY()

SRCS(
    crypto.go
)

GO_XTEST_SRCS(issue21104_test.go)

END()

RECURSE(
    aes
    cipher
    des
    dsa
    ecdh
    ecdsa
    ed25519
    elliptic
    hmac
    internal
    md5
    rand
    rc4
    rsa
    sha1
    sha256
    sha512
    subtle
    tls
    x509
)
