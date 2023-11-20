GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    block.go
    blocks.go
    decode_asm.go
)

GO_TEST_SRCS(decode_test.go)

GO_XTEST_SRCS(
    # block_test.go
)

IF (ARCH_X86_64)
    SRCS(decode_amd64.s)
ENDIF()

IF (ARCH_ARM64)
    SRCS(decode_arm64.s)
ENDIF()

END()

RECURSE(gotest)
