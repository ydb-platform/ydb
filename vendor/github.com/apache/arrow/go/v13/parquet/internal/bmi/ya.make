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

SRCS(bmi.go)

IF (ARCH_X86_64)
    SRCS(
        bitmap_bmi2_amd64.go
        bitmap_bmi2_amd64.s
        bmi_amd64.go
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM64)
    SRCS(
        bitmap_neon_arm64.go
        bitmap_neon_arm64.s
        bmi_arm64.go
    )
ENDIF()

IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
        bitmap_neon_arm64.go
        bitmap_neon_arm64.s
        bmi_arm64.go
    )
ENDIF()

END()
