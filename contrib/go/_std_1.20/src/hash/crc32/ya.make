GO_LIBRARY()

SRCS(
    crc32.go
    crc32_generic.go
)

IF (ARCH_ARM64)
    SRCS(
        crc32_arm64.go
        crc32_arm64.s
    )
ENDIF()

IF (ARCH_X86_64)
    SRCS(
        crc32_amd64.go
        crc32_amd64.s
    )
ENDIF()

END()
