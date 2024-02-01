GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		goos.go
		unix.go
		zgoos_darwin.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		goos.go
		unix.go
		zgoos_darwin.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		goos.go
		unix.go
		zgoos_linux.go
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		goos.go
		unix.go
		zgoos_linux.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		goos.go
		nonunix.go
		zgoos_windows.go
    )
ENDIF()
END()
