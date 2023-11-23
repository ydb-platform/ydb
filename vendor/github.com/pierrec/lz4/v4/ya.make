GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    lz4.go
    options.go
    options_gen.go
    reader.go
    state.go
    state_gen.go
    writer.go
)

GO_XTEST_SRCS(
    # bench_test.go
    # example_test.go
    # reader_test.go
    # writer_test.go
)

END()

RECURSE(
    gotest
    internal
)
