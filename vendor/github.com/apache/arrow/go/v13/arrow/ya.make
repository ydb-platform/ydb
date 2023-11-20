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
    compare.go
    datatype.go
    datatype_binary.go
    datatype_encoded.go
    datatype_extension.go
    datatype_fixedwidth.go
    datatype_nested.go
    datatype_null.go
    datatype_numeric.gen.go
    doc.go
    errors.go
    record.go
    schema.go
    table.go
    type_string.go
    type_traits_boolean.go
    type_traits_decimal128.go
    type_traits_decimal256.go
    type_traits_float16.go
    type_traits_interval.go
    type_traits_numeric.gen.go
    unionmode_string.go
)

GO_TEST_SRCS(
    compare_test.go
    datatype_nested_test.go
    schema_test.go
)

GO_XTEST_SRCS(
    datatype_binary_test.go
    datatype_extension_test.go
    datatype_fixedwidth_test.go
    datatype_null_test.go
    example_test.go
    type_traits_numeric.gen_test.go
    type_traits_test.go
)

END()

RECURSE(
    array
    arrio
    bitutil
    compute
    csv
    decimal128
    decimal256
    encoded
    endian
    flight
    float16
    internal
    ipc
    math
    memory
    scalar
    tensor
    util
)
