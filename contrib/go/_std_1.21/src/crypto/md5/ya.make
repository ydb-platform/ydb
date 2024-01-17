GO_LIBRARY()

SRCS(
    md5.go
    md5block.go
    md5block_decl.go
)

GO_TEST_SRCS(md5_test.go)

GO_XTEST_SRCS(example_test.go)

IF (ARCH_X86_64)
    SRCS(
        md5block_amd64.s
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        md5block_arm64.s
    )
ENDIF()

END()

RECURSE(
)
