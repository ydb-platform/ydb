GO_LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause AND
    MIT
)

SRCS(
    bitreader.go
    bitwriter.go
    blockdec.go
    blockenc.go
    blocktype_string.go
    bytebuf.go
    bytereader.go
    decodeheader.go
    decoder.go
    decoder_options.go
    dict.go
    enc_base.go
    enc_best.go
    enc_better.go
    enc_dfast.go
    enc_fast.go
    encoder.go
    encoder_options.go
    framedec.go
    frameenc.go
    fse_decoder.go
    fse_encoder.go
    fse_predefined.go
    hash.go
    history.go
    seqdec.go
    seqenc.go
    snappy.go
    zip.go
    zstd.go
)

GO_TEST_SRCS(
    decodeheader_test.go
    decoder_test.go
    dict_test.go
    encoder_options_test.go
    encoder_test.go
    fuzz_test.go
    seqdec_test.go
    snappy_test.go
    zstd_test.go
)

GO_XTEST_SRCS(
    example_test.go
    zip_test.go
)

IF (ARCH_X86_64)
    SRCS(
        fse_decoder_amd64.go
        fse_decoder_amd64.s
        matchlen_amd64.go
        matchlen_amd64.s
        seqdec_amd64.go
        seqdec_amd64.s
    )

    GO_TEST_SRCS(seqdec_amd64_test.go)
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        fse_decoder_generic.go
        matchlen_generic.go
        seqdec_generic.go
    )
ENDIF()

END()

RECURSE(
    gotest
    internal
)
