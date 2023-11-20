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
    array.go
    binary.go
    binarybuilder.go
    boolean.go
    booleanbuilder.go
    bufferbuilder.go
    bufferbuilder_byte.go
    bufferbuilder_numeric.gen.go
    builder.go
    compare.go
    concat.go
    data.go
    decimal128.go
    decimal256.go
    dictionary.go
    diff.go
    doc.go
    encoded.go
    extension.go
    extension_builder.go
    fixed_size_list.go
    fixedsize_binary.go
    fixedsize_binarybuilder.go
    float16.go
    float16_builder.go
    interval.go
    json_reader.go
    list.go
    map.go
    null.go
    numeric.gen.go
    numericbuilder.gen.go
    record.go
    string.go
    struct.go
    table.go
    union.go
    util.go
)

GO_TEST_SRCS(
    binary_test.go
    bufferbuilder_numeric_test.go
    builder_test.go
    data_test.go
    fixedsize_binarybuilder_test.go
)

GO_XTEST_SRCS(
    array_test.go
    binarybuilder_test.go
    boolean_test.go
    booleanbuilder_test.go
    compare_test.go
    concat_test.go
    decimal128_test.go
    decimal256_test.go
    decimal_test.go
    dictionary_test.go
    diff_test.go
    encoded_test.go
    extension_test.go
    fixed_size_list_test.go
    fixedsize_binary_test.go
    float16_builder_test.go
    interval_test.go
    json_reader_test.go
    list_test.go
    map_test.go
    null_test.go
    numeric_test.go
    numericbuilder.gen_test.go
    record_test.go
    string_test.go
    struct_test.go
    table_test.go
    union_test.go
    util_test.go
)

END()
