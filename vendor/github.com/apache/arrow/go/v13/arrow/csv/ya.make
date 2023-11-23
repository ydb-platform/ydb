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
    common.go
    reader.go
    transformer.go
    writer.go
)

GO_XTEST_SRCS(
    reader_test.go
    writer_test.go
)

END()
