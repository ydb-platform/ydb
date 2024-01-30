GO_LIBRARY()
IF (FALSE)
    MESSAGE(FATAL this shall never happen)

ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		bits_go1.13.go
		poly1305.go
		sum_amd64.go
		sum_amd64.s
		sum_generic.go
    )
ELSEIF (OS_LINUX AND ARCH_ARM64)
    SRCS(
		bits_go1.13.go
		mac_noasm.go
		poly1305.go
		sum_generic.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		bits_go1.13.go
		mac_noasm.go
		poly1305.go
		sum_generic.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		bits_go1.13.go
		poly1305.go
		sum_amd64.go
		sum_amd64.s
		sum_generic.go
    )
ELSEIF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		bits_go1.13.go
		mac_noasm.go
		poly1305.go
		sum_generic.go
    )
ELSEIF (OS_DARWIN AND ARCH_AARCH64)
    SRCS(
		bits_go1.13.go
		mac_noasm.go
		poly1305.go
		sum_generic.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		bits_go1.13.go
		poly1305.go
		sum_amd64.go
		sum_amd64.s
		sum_generic.go
    )
ELSEIF (OS_WINDOWS AND ARCH_ARM64)
    SRCS(
		bits_go1.13.go
		mac_noasm.go
		poly1305.go
		sum_generic.go
    )
ELSEIF (OS_WINDOWS AND ARCH_AARCH64)
    SRCS(
		bits_go1.13.go
		mac_noasm.go
		poly1305.go
		sum_generic.go
    )
ENDIF()
END()
