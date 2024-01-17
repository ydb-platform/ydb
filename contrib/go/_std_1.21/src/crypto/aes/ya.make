GO_LIBRARY()

SRCS(
    aes_gcm.go
    block.go
    cipher.go
    cipher_asm.go
    const.go
    modes.go
)

GO_TEST_SRCS(
    aes_test.go
    modes_test.go
)

IF (ARCH_X86_64)
    SRCS(
        asm_amd64.s
        gcm_amd64.s
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        asm_arm64.s
        gcm_arm64.s
    )
ENDIF()

END()

RECURSE(
)
