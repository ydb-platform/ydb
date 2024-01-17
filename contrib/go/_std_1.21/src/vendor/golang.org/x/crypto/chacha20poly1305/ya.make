GO_LIBRARY()

SRCS(
    chacha20poly1305.go
    chacha20poly1305_generic.go
    xchacha20poly1305.go
)

IF (ARCH_X86_64)
    SRCS(
        chacha20poly1305_amd64.go
        chacha20poly1305_amd64.s
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        chacha20poly1305_noasm.go
    )
ENDIF()

END()
