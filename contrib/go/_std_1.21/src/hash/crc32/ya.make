GO_LIBRARY()

SRCS(
    crc32.go
    crc32_generic.go
)

GO_TEST_SRCS(crc32_test.go)

GO_XTEST_SRCS(example_test.go)

IF (ARCH_X86_64)
    SRCS(
        crc32_amd64.go
        crc32_amd64.s
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        crc32_arm64.go
        crc32_arm64.s
    )
ENDIF()

END()

RECURSE(
)
