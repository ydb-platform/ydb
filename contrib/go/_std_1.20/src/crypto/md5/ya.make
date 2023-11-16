GO_LIBRARY()

SRCS(
    md5.go
    md5block.go
    md5block_decl.go
)

IF (ARCH_ARM64)
    SRCS(
        md5block_arm64.s
    )
ENDIF()

IF (ARCH_X86_64)
    SRCS(
        md5block_amd64.s
    )
ENDIF()

END()
