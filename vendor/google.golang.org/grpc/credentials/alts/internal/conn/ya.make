GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    aeadrekey.go
    aes128gcm.go
    aes128gcmrekey.go
    common.go
    counter.go
    record.go
    utils.go
)

GO_TEST_SRCS(
    aeadrekey_test.go
    aes128gcm_test.go
    aes128gcmrekey_test.go
    counter_test.go
    record_test.go
)

END()

RECURSE(gotest)
