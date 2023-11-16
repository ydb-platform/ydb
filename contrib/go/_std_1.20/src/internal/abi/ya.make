GO_LIBRARY()

SRCS(
    abi.go
    abi_test.s
)

IF (ARCH_ARM64)
    SRCS(
        abi_arm64.go
    )
ENDIF()

IF (ARCH_X86_64)
    SRCS(
        abi_amd64.go
    )
ENDIF()

END()
