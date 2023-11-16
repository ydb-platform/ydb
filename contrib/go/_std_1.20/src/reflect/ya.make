GO_LIBRARY()

SRCS(
    abi.go
    deepequal.go
    float32reg_generic.go
    makefunc.go
    swapper.go
    type.go
    value.go
    visiblefields.go
)

IF (ARCH_ARM64)
    SRCS(
        asm_arm64.s
    )
ENDIF()

IF (ARCH_X86_64)
    SRCS(
        asm_amd64.s
    )
ENDIF()

END()

RECURSE(
    internal
)
