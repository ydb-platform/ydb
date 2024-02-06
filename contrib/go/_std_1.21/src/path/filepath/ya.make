GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		match.go
		path.go
		path_nonwindows.go
		path_unix.go
		symlink.go
		symlink_unix.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		match.go
		path.go
		path_nonwindows.go
		path_unix.go
		symlink.go
		symlink_unix.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		match.go
		path.go
		path_nonwindows.go
		path_unix.go
		symlink.go
		symlink_unix.go
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		match.go
		path.go
		path_nonwindows.go
		path_unix.go
		symlink.go
		symlink_unix.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		match.go
		path.go
		path_windows.go
		symlink.go
		symlink_windows.go
    )
ENDIF()
END()
