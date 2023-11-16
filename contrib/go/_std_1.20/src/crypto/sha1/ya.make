GO_LIBRARY()

SRCS(
    sha1.go
    sha1block.go
)

IF (ARCH_ARM64)
    SRCS(
        sha1block_arm64.go
        sha1block_arm64.s
    )
ENDIF()

IF (ARCH_X86_64)
    SRCS(
        sha1block_amd64.go
        sha1block_amd64.s
    )
ENDIF()

IF (CGO_ENABLED)
    SRCS(
        boring.go
    )
ELSE()
    SRCS(
        notboring.go
    )
ENDIF()

END()
