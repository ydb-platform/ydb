GO_LIBRARY()
IF (FALSE)
    MESSAGE(FATAL this shall never happen)

ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		chacha_generic.go
		chacha_noasm.go
		xor.go
    )
ELSEIF (OS_LINUX AND ARCH_ARM64)
    SRCS(
		chacha_arm64.go
		chacha_arm64.s
		chacha_generic.go
		xor.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		chacha_arm64.go
		chacha_arm64.s
		chacha_generic.go
		xor.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		chacha_generic.go
		chacha_noasm.go
		xor.go
    )
ELSEIF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		chacha_arm64.go
		chacha_arm64.s
		chacha_generic.go
		xor.go
    )
ELSEIF (OS_DARWIN AND ARCH_AARCH64)
    SRCS(
		chacha_arm64.go
		chacha_arm64.s
		chacha_generic.go
		xor.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		chacha_generic.go
		chacha_noasm.go
		xor.go
    )
ELSEIF (OS_WINDOWS AND ARCH_ARM64)
    SRCS(
		chacha_arm64.go
		chacha_arm64.s
		chacha_generic.go
		xor.go
    )
ELSEIF (OS_WINDOWS AND ARCH_AARCH64)
    SRCS(
		chacha_arm64.go
		chacha_arm64.s
		chacha_generic.go
		xor.go
    )
ENDIF()
END()
