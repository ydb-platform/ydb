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
    bitmap_ops.go
    bitmaps.go
    bitutil.go
    endian_default.go
)

GO_XTEST_SRCS(
    bitmaps_test.go
    bitutil_test.go
)

IF (ARCH_X86_64)
    SRCS(
        bitmap_ops_amd64.go
        bitmap_ops_avx2_amd64.go
        bitmap_ops_avx2_amd64.s
        bitmap_ops_sse4_amd64.go
        bitmap_ops_sse4_amd64.s
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM64)
    SRCS(bitmap_ops_arm64.go)
ENDIF()

IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(bitmap_ops_arm64.go)
ENDIF()

END()
