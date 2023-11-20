GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    block.go
    debug_stub.go
    errors.go
    lz4.go
    lz4_go1.10.go
    reader.go
    reader_legacy.go
    writer.go
    writer_legacy.go
)

GO_TEST_SRCS(decode_test.go)

GO_XTEST_SRCS(
    # bench_test.go
    # block_test.go
    example_test.go
    reader_legacy_test.go
    # reader_test.go
    writer_legacy_test.go
    # writer_test.go
)

IF (ARCH_X86_64)
    SRCS(
        decode_amd64.go
        decode_amd64.s
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM64)
    SRCS(decode_other.go)
ENDIF()

IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(decode_other.go)
ENDIF()

END()

RECURSE(
    # cmd
    # fuzz
    gotest
    internal
)
