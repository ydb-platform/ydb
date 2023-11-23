GO_LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-2-Clause AND
    BSD-3-Clause AND
    BSL-1.0 AND
    CC-BY-3.0 AND
    HPND AND
    MIT AND
    NCSA AND
    OpenSSL AND
    Zlib
)

SRCS(
    hash_util.go
    kernel.go
    span.go
    utils.go
)

GO_XTEST_SRCS(
    kernel_test.go
    span_test.go
    utils_test.go
)

END()
