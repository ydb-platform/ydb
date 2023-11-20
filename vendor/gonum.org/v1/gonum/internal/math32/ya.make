GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    doc.go
    math.go
    signbit.go
)

IF (ARCH_X86_64)
    SRCS(
        sqrt_amd64.go
        sqrt_amd64.s
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        sqrt_arm64.go
        sqrt_arm64.s
    )
ENDIF()

END()
