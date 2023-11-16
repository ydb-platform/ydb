GO_LIBRARY()

SRCS(
    fe.go
    fe_generic.go
)

IF (ARCH_ARM64)
    SRCS(
        fe_amd64_noasm.go
        fe_arm64.go
        fe_arm64.s
    )
ENDIF()

IF (ARCH_X86_64)
    SRCS(
        fe_amd64.go
        fe_amd64.s
        fe_arm64_noasm.go
    )
ENDIF()

END()

#RECURSE(
#    _asm
#)
