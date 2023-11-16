GO_LIBRARY()

LICENSE(MIT)

SRCS(
    accessors.go
    conversions.go
    doc.go
    map.go
    mutations.go
    security.go
    tests.go
    type_specific.go
    type_specific_codegen.go
    value.go
)

GO_XTEST_SRCS(
    accessors_test.go
    conversions_test.go
    fixture_test.go
    map_test.go
    mutations_test.go
    security_test.go
    simple_example_test.go
    tests_test.go
    type_specific_codegen_test.go
    type_specific_test.go
    value_test.go
)

END()

RECURSE(
    gotest
)
