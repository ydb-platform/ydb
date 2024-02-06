GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		fe.go
		fe_amd64_noasm.go
		fe_arm64.go
		fe_arm64.s
		fe_generic.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		fe.go
		fe_amd64.go
		fe_amd64.s
		fe_arm64_noasm.go
		fe_generic.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		fe.go
		fe_amd64_noasm.go
		fe_arm64.go
		fe_arm64.s
		fe_generic.go
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		fe.go
		fe_amd64.go
		fe_amd64.s
		fe_arm64_noasm.go
		fe_generic.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		fe.go
		fe_amd64.go
		fe_amd64.s
		fe_arm64_noasm.go
		fe_generic.go
    )
ENDIF()
END()


RECURSE(
	# _asm
)
