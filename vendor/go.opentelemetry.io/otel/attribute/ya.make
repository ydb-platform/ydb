GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    doc.go
    encoder.go
    filter.go
    iterator.go
    key.go
    kv.go
    set.go
    type_string.go
    value.go
)

GO_TEST_SRCS(filter_test.go)

GO_XTEST_SRCS(
    benchmark_test.go
    iterator_test.go
    key_test.go
    kv_test.go
    set_test.go
    value_test.go
)

END()

RECURSE(gotest)
