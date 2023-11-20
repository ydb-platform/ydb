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
    doc.go
    encryption_properties.go
    reader_properties.go
    types.go
    version_string.go
    writer_properties.go
)

GO_XTEST_SRCS(
    encryption_properties_test.go
    encryption_read_config_test.go
    encryption_write_config_test.go
    reader_writer_properties_test.go
)

END()

RECURSE(
    cmd
    compress
    file
    internal
    metadata
    pqarrow
    schema
)
