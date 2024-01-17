GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    methods.go
    proto.go
    source.go
    source_gen.go
    type.go
    value.go
    value_equal.go
    value_union.go
    value_unsafe.go
)

GO_TEST_SRCS(
    proto_test.go
    source_test.go
    value_test.go
)

END()

RECURSE(gotest)
