GO_LIBRARY()

SRCS(
    nat.go
)

IF (ARCH_ARM64)
    SRCS(
        nat_noasm.go
    )
ENDIF()

IF (ARCH_X86_64)
    SRCS(
        nat_amd64.go
        nat_amd64.s
    )
ENDIF()

END()
