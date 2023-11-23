GO_LIBRARY()

SRCS(
    boring.go
    sha1.go
    sha1block.go
)

GO_TEST_SRCS(sha1_test.go)

GO_XTEST_SRCS(example_test.go)

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

IF (OS_LINUX AND ARCH_X86_64)
    GO_XTEST_SRCS(issue15617_test.go)
ENDIF()

IF (OS_DARWIN AND ARCH_X86_64)
    GO_XTEST_SRCS(issue15617_test.go)
ENDIF()

END()

RECURSE(
)
