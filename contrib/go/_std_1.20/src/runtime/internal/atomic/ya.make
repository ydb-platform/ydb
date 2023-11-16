GO_LIBRARY()

SRCS(
    doc.go
    stubs.go
    types.go
    types_64bit.go
    unaligned.go
)

IF (ARCH_ARM64)
    SRCS(
        atomic_arm64.go
        atomic_arm64.s
    )
ENDIF()

IF (ARCH_X86_64)
    SRCS(
        atomic_amd64.go
        atomic_amd64.s
    )
ENDIF()

END()
