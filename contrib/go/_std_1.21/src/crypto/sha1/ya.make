GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64 OR OS_LINUX AND ARCH_AARCH64)
    SRCS(
		notboring.go
		sha1.go
		sha1block.go
		sha1block_arm64.go
		sha1block_arm64.s
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64 OR OS_LINUX AND ARCH_X86_64 OR OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		notboring.go
		sha1.go
		sha1block.go
		sha1block_amd64.go
		sha1block_amd64.s
    )
ENDIF()
END()
