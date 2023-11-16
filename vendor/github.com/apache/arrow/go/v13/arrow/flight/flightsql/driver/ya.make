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
    config.go
    driver.go
    errors.go
    utils.go
)

GO_XTEST_SRCS(
    config_test.go
    driver_test.go
)

END()
