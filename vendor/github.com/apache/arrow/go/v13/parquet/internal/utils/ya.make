GO_LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-2-Clause AND
    BSD-3-Clause AND
    BSL-1.0 AND
    CC-BY-3.0 AND
    HPND AND
    MIT AND
    NCSA AND
    OpenSSL AND
    Zlib
)

SRCS(
    bit_packing_default.go
    bit_reader.go
    bit_writer.go
    bitmap_writer.go
    dictionary.go
    rle.go
    typed_rle_dict.gen.go
    unpack_bool.go
    write_utils.go
)

GO_XTEST_SRCS(
    bit_benchmark_test.go
    bit_reader_test.go
    bitmap_writer_test.go
)

IF (ARCH_X86_64)
    SRCS(
        bit_packing_amd64.go
        bit_packing_avx2_amd64.go
        bit_packing_avx2_amd64.s
        clib_amd64.s
        unpack_bool_amd64.go
        unpack_bool_avx2_amd64.go
        unpack_bool_avx2_amd64.s
        unpack_bool_sse4_amd64.go
        unpack_bool_sse4_amd64.s
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM64)
    SRCS(
        bit_packing_arm64.go
        bit_packing_neon_arm64.go
        bit_packing_neon_arm64.s
        unpack_bool_arm64.go
        unpack_bool_neon_arm64.go
        unpack_bool_neon_arm64.s
    )
ENDIF()

IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
        bit_packing_arm64.go
        bit_packing_neon_arm64.go
        bit_packing_neon_arm64.s
        unpack_bool_arm64.go
        unpack_bool_neon_arm64.go
        unpack_bool_neon_arm64.s
    )
ENDIF()

END()
