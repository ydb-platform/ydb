GO_LIBRARY()

SRCS(
    sha512.go
    sha512block.go
)

GO_TEST_SRCS(sha512_test.go)

IF (ARCH_X86_64)
    SRCS(
        sha512block_amd64.go
        sha512block_amd64.s
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        sha512block_arm64.go
        sha512block_arm64.s
    )
ENDIF()

END()

RECURSE(
)
