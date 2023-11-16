GO_LIBRARY()

SRCS(
    constant_time.go
    xor.go
)

IF (ARCH_ARM64)
    SRCS(
        xor_arm64.go
        xor_arm64.s
    )
ENDIF()

IF (ARCH_X86_64)
    SRCS(
        xor_amd64.go
        xor_amd64.s
    )
ENDIF()

END()
