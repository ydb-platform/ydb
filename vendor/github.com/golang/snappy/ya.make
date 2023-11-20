GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    decode.go
    decode_asm.go
    encode.go
    encode_asm.go
    snappy.go
)

GO_TEST_SRCS(
    golden_test.go
    snappy_test.go
)

IF (ARCH_X86_64)
    SRCS(
        decode_amd64.s
        encode_amd64.s
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM64)
    SRCS(
        decode_arm64.s
        encode_arm64.s
    )
ENDIF()

IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
        decode_arm64.s
        encode_arm64.s
    )
ENDIF()

END()

RECURSE(
    cmd
    gotest
)
