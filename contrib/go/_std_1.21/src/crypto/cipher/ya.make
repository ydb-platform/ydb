GO_LIBRARY()

SRCS(
    cbc.go
    cfb.go
    cipher.go
    ctr.go
    gcm.go
    io.go
    ofb.go
)

GO_TEST_SRCS(export_test.go)

GO_XTEST_SRCS(
    benchmark_test.go
    cbc_aes_test.go
    cfb_test.go
    cipher_test.go
    common_test.go
    ctr_aes_test.go
    ctr_test.go
    example_test.go
    gcm_test.go
    ofb_test.go
)

END()

RECURSE(
)
