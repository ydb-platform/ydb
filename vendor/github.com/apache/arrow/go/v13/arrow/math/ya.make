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
    doc.go
    float64.go
    int64.go
    uint64.go
)

GO_XTEST_SRCS(
    float64_test.go
    int64_test.go
    uint64_test.go
)

IF (ARCH_X86_64)
    SRCS(
        float64_amd64.go
        float64_avx2_amd64.go
        float64_avx2_amd64.s
        float64_sse4_amd64.go
        float64_sse4_amd64.s
        int64_amd64.go
        int64_avx2_amd64.go
        int64_avx2_amd64.s
        int64_sse4_amd64.go
        int64_sse4_amd64.s
        math_amd64.go
        uint64_amd64.go
        uint64_avx2_amd64.go
        uint64_avx2_amd64.s
        uint64_sse4_amd64.go
        uint64_sse4_amd64.s
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM64)
    SRCS(
        float64_arm64.go
        float64_neon_arm64.go
        float64_neon_arm64.s
        int64_arm64.go
        int64_neon_arm64.go
        int64_neon_arm64.s
        math_arm64.go
        uint64_arm64.go
        uint64_neon_arm64.go
        uint64_neon_arm64.s
    )
ENDIF()

IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
        float64_arm64.go
        float64_neon_arm64.go
        float64_neon_arm64.s
        int64_arm64.go
        int64_neon_arm64.go
        int64_neon_arm64.s
        math_arm64.go
        uint64_arm64.go
        uint64_neon_arm64.go
        uint64_neon_arm64.s
    )
ENDIF()

END()
