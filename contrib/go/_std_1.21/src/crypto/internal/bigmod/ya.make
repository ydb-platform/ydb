GO_LIBRARY()

SRCS(
    nat.go
    nat_asm.go
)

GO_TEST_SRCS(nat_test.go)

IF (ARCH_X86_64)
    SRCS(
        nat_amd64.s
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        nat_arm64.s
    )
ENDIF()

END()

RECURSE(
)
