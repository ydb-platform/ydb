GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		elliptic.go
		nistec.go
		nistec_p256.go
		params.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		elliptic.go
		nistec.go
		nistec_p256.go
		params.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		elliptic.go
		nistec.go
		nistec_p256.go
		params.go
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		elliptic.go
		nistec.go
		nistec_p256.go
		params.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		elliptic.go
		nistec.go
		nistec_p256.go
		params.go
    )
ENDIF()
END()
