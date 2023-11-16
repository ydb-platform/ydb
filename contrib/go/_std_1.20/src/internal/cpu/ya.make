GO_LIBRARY()

SRCS(
    cpu.go
    cpu.s
)

IF (ARCH_ARM64)
    SRCS(
        cpu_arm64.go
        cpu_arm64.s
        cpu_no_name.go
    )
ENDIF()

IF (ARCH_X86_64)
    SRCS(
        cpu_x86.go
        cpu_x86.s
    )
ENDIF()

IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
        cpu_arm64_darwin.go
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM64)
    SRCS(
        cpu_arm64_hwcap.go
        cpu_arm64_linux.go
    )
ENDIF()

IF (OS_WINDOWS AND ARCH_ARM64)
    SRCS(
        cpu_arm64_other.go
    )
ENDIF()

END()
