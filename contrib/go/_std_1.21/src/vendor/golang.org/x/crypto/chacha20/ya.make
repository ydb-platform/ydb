GO_LIBRARY()

SRCS(
    chacha_generic.go
    xor.go
)

IF (ARCH_X86_64)
    SRCS(
        chacha_noasm.go
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        chacha_arm64.go
        chacha_arm64.s
    )
ENDIF()

END()
