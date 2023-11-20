GO_LIBRARY()

SRCS(
    builder.go
    clone.go
    compare.go
    reader.go
    replace.go
    search.go
    strings.go
)

GO_TEST_SRCS(export_test.go)

GO_XTEST_SRCS(
    builder_test.go
    clone_test.go
    compare_test.go
    example_test.go
    reader_test.go
    replace_test.go
    search_test.go
    strings_test.go
)

END()

RECURSE(
)
