GO_LIBRARY()

SRCS(
    constant_time.go
    xor.go
)

GO_TEST_SRCS(constant_time_test.go)

GO_XTEST_SRCS(xor_test.go)

IF (ARCH_X86_64)
    SRCS(
        xor_amd64.go
        xor_amd64.s
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        xor_arm64.go
        xor_arm64.s
    )
ENDIF()

END()

RECURSE(
)
