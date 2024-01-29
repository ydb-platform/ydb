GO_LIBRARY()
IF (FALSE)
    MESSAGE(FATAL this shall never happen)

ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		constant_time.go
		xor.go
		xor_amd64.go
		xor_amd64.s
    )
ELSEIF (OS_LINUX AND ARCH_ARM64)
    SRCS(
		constant_time.go
		xor.go
		xor_arm64.go
		xor_arm64.s
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		constant_time.go
		xor.go
		xor_arm64.go
		xor_arm64.s
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		constant_time.go
		xor.go
		xor_amd64.go
		xor_amd64.s
    )
ELSEIF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		constant_time.go
		xor.go
		xor_arm64.go
		xor_arm64.s
    )
ELSEIF (OS_DARWIN AND ARCH_AARCH64)
    SRCS(
		constant_time.go
		xor.go
		xor_arm64.go
		xor_arm64.s
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		constant_time.go
		xor.go
		xor_amd64.go
		xor_amd64.s
    )
ELSEIF (OS_WINDOWS AND ARCH_ARM64)
    SRCS(
		constant_time.go
		xor.go
		xor_arm64.go
		xor_arm64.s
    )
ELSEIF (OS_WINDOWS AND ARCH_AARCH64)
    SRCS(
		constant_time.go
		xor.go
		xor_arm64.go
		xor_arm64.s
    )
ENDIF()
END()
