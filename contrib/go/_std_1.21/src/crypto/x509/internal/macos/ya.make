GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		corefoundation.go
		corefoundation.s
		security.go
		security.s
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		corefoundation.go
		corefoundation.s
		security.go
		security.s
    )
ENDIF()
END()
