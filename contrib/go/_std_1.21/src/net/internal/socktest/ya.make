GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		switch.go
		switch_posix.go
		switch_unix.go
		sys_unix.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		switch.go
		switch_posix.go
		switch_unix.go
		sys_unix.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		switch.go
		switch_posix.go
		switch_unix.go
		sys_cloexec.go
		sys_unix.go
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		switch.go
		switch_posix.go
		switch_unix.go
		sys_cloexec.go
		sys_unix.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		switch.go
		switch_posix.go
		switch_windows.go
		sys_windows.go
    )
ENDIF()
END()
