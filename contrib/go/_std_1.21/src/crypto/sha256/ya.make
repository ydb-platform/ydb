GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		sha256.go
		sha256block.go
		sha256block_arm64.go
		sha256block_arm64.s
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		sha256.go
		sha256block.go
		sha256block_amd64.go
		sha256block_amd64.s
		sha256block_decl.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		sha256.go
		sha256block.go
		sha256block_arm64.go
		sha256block_arm64.s
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		sha256.go
		sha256block.go
		sha256block_amd64.go
		sha256block_amd64.s
		sha256block_decl.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		sha256.go
		sha256block.go
		sha256block_amd64.go
		sha256block_amd64.s
		sha256block_decl.go
    )
ENDIF()
END()
