GO_LIBRARY()

SRCS(
    boring.go
    sha1.go
    sha1block.go
)

IF (ARCH_X86_64)
    SRCS(
        sha1block_amd64.go
        sha1block_amd64.s
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        sha1block_arm64.go
        sha1block_arm64.s
    )
ENDIF()

END()
