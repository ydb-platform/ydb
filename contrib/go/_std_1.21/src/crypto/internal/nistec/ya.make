GO_LIBRARY()

SRCS(
    nistec.go
    p224.go
    p224_sqrt.go
    p256_asm.go
    p256_ordinv.go
    p384.go
    p521.go
)

GO_TEST_SRCS(p256_asm_table_test.go)

GO_XTEST_SRCS(
    nistec_test.go
    p256_ordinv_test.go
)

IF (ARCH_X86_64)
    SRCS(
        p256_asm_amd64.s
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        p256_asm_arm64.s
    )
ENDIF()

GO_EMBED_PATTERN(p256_asm_table.bin)

END()

RECURSE(
    fiat
)
