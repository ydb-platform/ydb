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
    buf_reader.go
    endians_default.go
    math.go
    min_max.go
    transpose_ints.go
    transpose_ints_def.go
)

GO_XTEST_SRCS(transpose_ints_test.go)

IF (ARCH_X86_64)
    SRCS(
        min_max_amd64.go
        min_max_avx2_amd64.go
        min_max_avx2_amd64.s
        min_max_sse4_amd64.go
        min_max_sse4_amd64.s
        transpose_ints_amd64.go
        transpose_ints_avx2_amd64.go
        transpose_ints_avx2_amd64.s
        transpose_ints_sse4_amd64.go
        transpose_ints_sse4_amd64.s
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM64)
    SRCS(
        min_max_arm64.go
        min_max_neon_arm64.go
        min_max_neon_arm64.s
        transpose_ints_arm64.go
    )
ENDIF()

IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
        min_max_arm64.go
        min_max_neon_arm64.go
        min_max_neon_arm64.s
        transpose_ints_arm64.go
    )
ENDIF()

END()
