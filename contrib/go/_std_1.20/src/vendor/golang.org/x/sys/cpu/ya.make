GO_LIBRARY()

SRCS(
    byteorder.go
    cpu.go
)

IF (ARCH_ARM64)
    SRCS(
        cpu_arm64.go
        cpu_arm64.s
        cpu_gc_arm64.go
    )
ENDIF()

IF (ARCH_X86_64)
    SRCS(
        cpu_gc_x86.go
        cpu_x86.go
        cpu_x86.s
    )
ENDIF()

IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
        cpu_other_arm64.go
    )
ENDIF()

IF (OS_LINUX)
    SRCS(
        hwcap_linux.go
    )

    IF (ARCH_ARM64)
        SRCS(
            cpu_linux_arm64.go
        )
    ENDIF()

    IF (ARCH_X86_64)
        SRCS(
            cpu_linux_noinit.go
        )
    ENDIF()
ENDIF()

IF (OS_WINDOWS AND ARCH_ARM64)
    SRCS(
        cpu_other_arm64.go
    )
ENDIF()

END()
