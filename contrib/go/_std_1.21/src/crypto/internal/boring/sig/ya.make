GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		sig.go
		sig_other.s
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		sig.go
		sig_amd64.s
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		sig.go
		sig_other.s
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		sig.go
		sig_amd64.s
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		sig.go
		sig_amd64.s
    )
ENDIF()
END()
