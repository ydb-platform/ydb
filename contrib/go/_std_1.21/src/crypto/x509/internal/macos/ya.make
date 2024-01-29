GO_LIBRARY()
IF (FALSE)
    MESSAGE(FATAL this shall never happen)

ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		corefoundation.go
		corefoundation.s
		security.go
		security.s
    )
ELSEIF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		corefoundation.go
		corefoundation.s
		security.go
		security.s
    )
ELSEIF (OS_DARWIN AND ARCH_AARCH64)
    SRCS(
		corefoundation.go
		corefoundation.s
		security.go
		security.s
    )
ENDIF()
END()
