GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		consts.go
		consts_norace.go
		intrinsics.go
		nih.go
		sys.go
		zversion.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		consts.go
		consts_norace.go
		intrinsics.go
		nih.go
		sys.go
		zversion.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		consts.go
		consts_norace.go
		intrinsics.go
		nih.go
		sys.go
		zversion.go
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		consts.go
		consts_norace.go
		intrinsics.go
		nih.go
		sys.go
		zversion.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		consts.go
		consts_norace.go
		intrinsics.go
		nih.go
		sys.go
		zversion.go
    )
ENDIF()
END()
