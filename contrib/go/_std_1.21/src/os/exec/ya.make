GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64 OR OS_DARWIN AND ARCH_X86_64 OR OS_LINUX AND ARCH_AARCH64 OR OS_LINUX AND ARCH_X86_64)
    SRCS(
		exec.go
		exec_unix.go
		lp_unix.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		exec.go
		exec_windows.go
		lp_windows.go
    )
ENDIF()
END()


RECURSE(
	internal
)
