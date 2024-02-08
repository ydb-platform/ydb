GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		ecdsa.go
		ecdsa_legacy.go
		ecdsa_noasm.go
		notboring.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		ecdsa.go
		ecdsa_legacy.go
		ecdsa_noasm.go
		notboring.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		ecdsa.go
		ecdsa_legacy.go
		ecdsa_noasm.go
		notboring.go
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		ecdsa.go
		ecdsa_legacy.go
		ecdsa_noasm.go
		notboring.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		ecdsa.go
		ecdsa_legacy.go
		ecdsa_noasm.go
		notboring.go
    )
ENDIF()
END()
