GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64 OR OS_DARWIN AND ARCH_X86_64)
    SRCS(
		rand.go
		rand_getentropy.go
		rand_unix.go
		util.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64 OR OS_LINUX AND ARCH_X86_64)
    SRCS(
		rand.go
		rand_getrandom.go
		rand_unix.go
		util.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		rand.go
		rand_windows.go
		util.go
    )
ENDIF()
END()
