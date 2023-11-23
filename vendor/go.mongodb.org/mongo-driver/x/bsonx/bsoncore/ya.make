GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    array.go
    bson_arraybuilder.go
    bson_documentbuilder.go
    bsoncore.go
    document.go
    document_sequence.go
    element.go
    tables.go
    value.go
)

GO_TEST_SRCS(
    array_test.go
    bson_arraybuilder_test.go
    bson_documentbuilder_test.go
    bsoncore_test.go
    document_sequence_test.go
    document_test.go
    element_test.go
    value_test.go
)

END()

RECURSE(gotest)
