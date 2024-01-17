GO_LIBRARY()

SRCS(
    sha256.go
    sha256block.go
)

GO_TEST_SRCS(sha256_test.go)

GO_XTEST_SRCS(example_test.go)

IF (ARCH_X86_64)
    SRCS(
        sha256block_amd64.go
        sha256block_amd64.s
        sha256block_decl.go
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        sha256block_arm64.go
        sha256block_arm64.s
    )
ENDIF()

END()

RECURSE(
)
