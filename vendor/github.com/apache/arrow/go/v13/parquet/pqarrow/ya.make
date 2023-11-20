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
    column_readers.go
    doc.go
    encode_arrow.go
    encode_dict_compute.go
    file_reader.go
    file_writer.go
    path_builder.go
    properties.go
    schema.go
)

GO_TEST_SRCS(path_builder_test.go)

GO_XTEST_SRCS(
    encode_arrow_test.go
    encode_dictionary_test.go
    file_reader_test.go
    reader_writer_test.go
    schema_test.go
)

END()
