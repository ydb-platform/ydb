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
    column.go
    converted_types.go
    helpers.go
    logical_types.go
    node.go
    reflection.go
    schema.go
)

GO_TEST_SRCS(
    schema_element_test.go
    schema_flatten_test.go
)

GO_XTEST_SRCS(
    converted_types_test.go
    helpers_test.go
    logical_types_test.go
    reflection_test.go
    schema_test.go
)

END()
