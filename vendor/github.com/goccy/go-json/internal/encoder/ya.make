GO_LIBRARY()

LICENSE(MIT)

SRCS(
    code.go
    compact.go
    compiler.go
    compiler_norace.go
    context.go
    decode_rune.go
    encoder.go
    indent.go
    int.go
    map113.go
    opcode.go
    option.go
    optype.go
    query.go
    string.go
    string_table.go
)

GO_TEST_SRCS(encode_opcode_test.go)

END()

RECURSE(
    gotest
    vm
    vm_color
    vm_color_indent
    vm_indent
)
