GO_LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause AND
    MIT
)

SRCS(
    decode.go
    decode_asm.go
    dict.go
    encode.go
    encode_all.go
    encode_best.go
    encode_better.go
    index.go
    lz4convert.go
    lz4sconvert.go
    reader.go
    s2.go
    writer.go
)

GO_TEST_SRCS(
    decode_test.go
    dict_test.go
    encode_test.go
    fuzz_test.go
    lz4convert_test.go
    lz4sconvert_test.go
    s2_test.go
    writer_test.go
)

GO_XTEST_SRCS(
    examples_test.go
    index_test.go
)

IF (ARCH_X86_64)
    SRCS(
        decode_amd64.s
        encode_amd64.go
        encodeblock_amd64.go
        encodeblock_amd64.s
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        decode_arm64.s
        encode_go.go
    )
ENDIF()

END()

RECURSE(gotest)
