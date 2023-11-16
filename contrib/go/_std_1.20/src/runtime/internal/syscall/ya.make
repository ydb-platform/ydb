GO_LIBRARY()

BUILD_ONLY_IF(WARNING OS_LINUX)

IF (OS_LINUX)
    SRCS(
        defs_linux.go
        syscall_linux.go
    )

    IF (ARCH_ARM64)
        SRCS(
            asm_linux_arm64.s
            defs_linux_arm64.go
        )
    ENDIF()

    IF (ARCH_X86_64)
        SRCS(
            asm_linux_amd64.s
            defs_linux_amd64.go
        )
    ENDIF()
ENDIF()

END()
