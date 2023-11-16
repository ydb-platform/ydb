GO_LIBRARY()

SRCS(
    bits_go1.13.go
    poly1305.go
    sum_generic.go
)

IF (ARCH_ARM64)
    SRCS(
        mac_noasm.go
    )
ENDIF()

IF (ARCH_X86_64)
    SRCS(
        sum_amd64.go
        sum_amd64.s
    )
ENDIF()

END()
