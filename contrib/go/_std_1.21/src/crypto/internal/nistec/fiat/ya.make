GO_LIBRARY()

SRCS(
    p224.go
    p224_fiat64.go
    p224_invert.go
    p256.go
    p256_fiat64.go
    p256_invert.go
    p384.go
    p384_fiat64.go
    p384_invert.go
    p521.go
    p521_fiat64.go
    p521_invert.go
)

GO_XTEST_SRCS(fiat_test.go)

END()

RECURSE(
)
