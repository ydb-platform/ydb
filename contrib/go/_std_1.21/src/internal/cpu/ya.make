GO_LIBRARY()
IF (FALSE)
    MESSAGE(FATAL this shall never happen)

ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		cpu.go
		cpu.s
		cpu_x86.go
		cpu_x86.s
    )
ELSEIF (OS_LINUX AND ARCH_ARM64)
    SRCS(
		cpu.go
		cpu.s
		cpu_arm64.go
		cpu_arm64.s
		cpu_arm64_hwcap.go
		cpu_arm64_linux.go
		cpu_no_name.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		cpu.go
		cpu.s
		cpu_arm64.go
		cpu_arm64.s
		cpu_arm64_hwcap.go
		cpu_arm64_linux.go
		cpu_no_name.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		cpu.go
		cpu.s
		cpu_x86.go
		cpu_x86.s
    )
ELSEIF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		cpu.go
		cpu.s
		cpu_arm64.go
		cpu_arm64.s
		cpu_arm64_darwin.go
		cpu_no_name.go
    )
ELSEIF (OS_DARWIN AND ARCH_AARCH64)
    SRCS(
		cpu.go
		cpu.s
		cpu_arm64.go
		cpu_arm64.s
		cpu_arm64_darwin.go
		cpu_no_name.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		cpu.go
		cpu.s
		cpu_x86.go
		cpu_x86.s
    )
ELSEIF (OS_WINDOWS AND ARCH_ARM64)
    SRCS(
		cpu.go
		cpu.s
		cpu_arm64.go
		cpu_arm64.s
		cpu_arm64_other.go
		cpu_no_name.go
    )
ELSEIF (OS_WINDOWS AND ARCH_AARCH64)
    SRCS(
		cpu.go
		cpu.s
		cpu_arm64.go
		cpu_arm64.s
		cpu_arm64_other.go
		cpu_no_name.go
    )
ENDIF()
END()
