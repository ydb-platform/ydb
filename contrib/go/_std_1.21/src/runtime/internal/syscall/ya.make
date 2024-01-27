GO_LIBRARY()
IF (FALSE)
    MESSAGE(FATAL this shall never happen)

ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		asm_linux_amd64.s
		defs_linux_amd64.go
		syscall_linux.go
    )
ELSEIF (OS_LINUX AND ARCH_ARM64)
    SRCS(
		asm_linux_arm64.s
		defs_linux_arm64.go
		syscall_linux.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		asm_linux_arm64.s
		defs_linux_arm64.go
		syscall_linux.go
    )
ENDIF()
END()
