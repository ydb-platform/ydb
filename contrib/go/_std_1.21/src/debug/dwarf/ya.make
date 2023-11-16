GO_LIBRARY()

SRCS(
    attr_string.go
    buf.go
    class_string.go
    const.go
    entry.go
    line.go
    open.go
    tag_string.go
    type.go
    typeunit.go
    unit.go
)

GO_TEST_SRCS(
    dwarf5ranges_test.go
    export_test.go
)

GO_XTEST_SRCS(
    entry_test.go
    line_test.go
    type_test.go
)

END()

RECURSE(
)
