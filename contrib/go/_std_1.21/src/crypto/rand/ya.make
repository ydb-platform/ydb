GO_LIBRARY()
IF (FALSE)
    MESSAGE(FATAL this shall never happen)

ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		rand.go
		rand_getrandom.go
		rand_unix.go
		util.go
    )
ELSEIF (OS_LINUX AND ARCH_ARM64)
    SRCS(
		rand.go
		rand_getrandom.go
		rand_unix.go
		util.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		rand.go
		rand_getrandom.go
		rand_unix.go
		util.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		rand.go
		rand_getentropy.go
		rand_unix.go
		util.go
    )
ELSEIF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		rand.go
		rand_getentropy.go
		rand_unix.go
		util.go
    )
ELSEIF (OS_DARWIN AND ARCH_AARCH64)
    SRCS(
		rand.go
		rand_getentropy.go
		rand_unix.go
		util.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		rand.go
		rand_windows.go
		util.go
    )
ELSEIF (OS_WINDOWS AND ARCH_ARM64)
    SRCS(
		rand.go
		rand_windows.go
		util.go
    )
ELSEIF (OS_WINDOWS AND ARCH_AARCH64)
    SRCS(
		rand.go
		rand_windows.go
		util.go
    )
ENDIF()
END()
