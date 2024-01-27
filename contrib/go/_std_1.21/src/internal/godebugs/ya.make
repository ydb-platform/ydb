GO_LIBRARY()
IF (FALSE)
    MESSAGE(FATAL this shall never happen)

ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		table.go
    )
ELSEIF (OS_LINUX AND ARCH_ARM64)
    SRCS(
		table.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		table.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		table.go
    )
ELSEIF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		table.go
    )
ELSEIF (OS_DARWIN AND ARCH_AARCH64)
    SRCS(
		table.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		table.go
    )
ELSEIF (OS_WINDOWS AND ARCH_ARM64)
    SRCS(
		table.go
    )
ELSEIF (OS_WINDOWS AND ARCH_AARCH64)
    SRCS(
		table.go
    )
ENDIF()
END()
