GO_LIBRARY()
IF (FALSE)
    MESSAGE(FATAL this shall never happen)

ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		nat.go
		nat_amd64.s
		nat_asm.go
    )
ELSEIF (OS_LINUX AND ARCH_ARM64)
    SRCS(
		nat.go
		nat_arm64.s
		nat_asm.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		nat.go
		nat_arm64.s
		nat_asm.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		nat.go
		nat_amd64.s
		nat_asm.go
    )
ELSEIF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		nat.go
		nat_arm64.s
		nat_asm.go
    )
ELSEIF (OS_DARWIN AND ARCH_AARCH64)
    SRCS(
		nat.go
		nat_arm64.s
		nat_asm.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		nat.go
		nat_amd64.s
		nat_asm.go
    )
ELSEIF (OS_WINDOWS AND ARCH_ARM64)
    SRCS(
		nat.go
		nat_arm64.s
		nat_asm.go
    )
ELSEIF (OS_WINDOWS AND ARCH_AARCH64)
    SRCS(
		nat.go
		nat_arm64.s
		nat_asm.go
    )
ENDIF()
END()


RECURSE(
	# _asm
)
