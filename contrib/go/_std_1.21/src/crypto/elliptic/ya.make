GO_LIBRARY()

SRCS(
    elliptic.go
    nistec.go
    nistec_p256.go
    params.go
)

GO_TEST_SRCS(
    elliptic_test.go
    p224_test.go
    p256_test.go
)

END()

RECURSE(
)
