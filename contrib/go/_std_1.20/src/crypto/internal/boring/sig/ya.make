GO_LIBRARY()

SRCS(
    sig.go
)

IF (ARCH_ARM64)
    SRCS(
        sig_other.s
    )
ENDIF()

IF (ARCH_X86_64)
    SRCS(
        sig_amd64.s
    )
ENDIF()

END()
