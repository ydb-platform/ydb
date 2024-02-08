GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		cache.go
		stub.s
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		cache.go
		stub.s
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		cache.go
		stub.s
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		cache.go
		stub.s
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		cache.go
		stub.s
    )
ENDIF()
END()
