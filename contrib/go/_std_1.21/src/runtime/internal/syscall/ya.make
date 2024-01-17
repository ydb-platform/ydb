GO_LIBRARY()

BUILD_ONLY_IF(
    WARNING
    OS_LINUX
)

IF (OS_LINUX)
    SRCS(
        syscall_linux.go
    )

    GO_XTEST_SRCS(syscall_linux_test.go)
ENDIF()

IF (OS_LINUX AND ARCH_X86_64)
    SRCS(
        asm_linux_amd64.s
        defs_linux_amd64.go
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM64)
    SRCS(
        asm_linux_arm64.s
        defs_linux_arm64.go
    )
ENDIF()

END()

RECURSE(
)
