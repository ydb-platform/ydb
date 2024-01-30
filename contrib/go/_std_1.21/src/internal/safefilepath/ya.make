GO_LIBRARY()
IF (FALSE)
    MESSAGE(FATAL this shall never happen)

ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		path.go
		path_other.go
    )
ELSEIF (OS_LINUX AND ARCH_ARM64)
    SRCS(
		path.go
		path_other.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		path.go
		path_other.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		path.go
		path_other.go
    )
ELSEIF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		path.go
		path_other.go
    )
ELSEIF (OS_DARWIN AND ARCH_AARCH64)
    SRCS(
		path.go
		path_other.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		path.go
		path_windows.go
    )
ELSEIF (OS_WINDOWS AND ARCH_ARM64)
    SRCS(
		path.go
		path_windows.go
    )
ELSEIF (OS_WINDOWS AND ARCH_AARCH64)
    SRCS(
		path.go
		path_windows.go
    )
ENDIF()
END()
