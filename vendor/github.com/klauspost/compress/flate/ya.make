GO_LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause AND
    MIT
)

SRCS(
    deflate.go
    dict_decoder.go
    fast_encoder.go
    huffman_bit_writer.go
    huffman_code.go
    huffman_sortByFreq.go
    huffman_sortByLiteral.go
    inflate.go
    inflate_gen.go
    level1.go
    level2.go
    level3.go
    level4.go
    level5.go
    level6.go
    stateless.go
    token.go
)

GO_TEST_SRCS(
    deflate_test.go
    dict_decoder_test.go
    flate_test.go
    fuzz_test.go
    huffman_bit_writer_test.go
    inflate_test.go
    reader_test.go
    token_test.go
    writer_test.go
)

IF (ARCH_X86_64)
    SRCS(regmask_amd64.go)
ENDIF()

IF (ARCH_ARM64)
    SRCS(regmask_other.go)
ENDIF()

END()

RECURSE(gotest)
