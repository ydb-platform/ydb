GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    copier.go
    doc.go
    extjson_parser.go
    extjson_reader.go
    extjson_tables.go
    extjson_wrappers.go
    extjson_writer.go
    json_scanner.go
    mode.go
    reader.go
    value_reader.go
    value_writer.go
    writer.go
)

GO_TEST_SRCS(
    bsonrw_test.go
    copier_test.go
    extjson_parser_test.go
    extjson_reader_test.go
    extjson_writer_test.go
    json_scanner_test.go
    value_reader_test.go
    value_reader_writer_test.go
    value_writer_test.go
)

END()

RECURSE(
    bsonrwtest
    gotest
)
