GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    base.go
    cldr.go
    collate.go
    decode.go
    resolve.go
    slice.go
    xml.go
)

GO_TEST_SRCS(
    cldr_test.go
    collate_test.go
    data_test.go
    resolve_test.go
    slice_test.go
)

GO_XTEST_SRCS(examples_test.go)

END()

RECURSE(
    # gotest
)
