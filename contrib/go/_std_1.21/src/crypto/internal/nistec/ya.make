GO_LIBRARY()
IF (FALSE)
    MESSAGE(FATAL this shall never happen)

ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		nistec.go
		p224.go
		p224_sqrt.go
		p256_asm.go
		p256_asm_amd64.s
		p256_ordinv.go
		p384.go
		p521.go
    )
    
    GO_EMBED_PATTERN(p256_asm_table.bin)
ELSEIF (OS_LINUX AND ARCH_ARM64)
    SRCS(
		nistec.go
		p224.go
		p224_sqrt.go
		p256_asm.go
		p256_asm_arm64.s
		p256_ordinv.go
		p384.go
		p521.go
    )
    
    GO_EMBED_PATTERN(p256_asm_table.bin)
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		nistec.go
		p224.go
		p224_sqrt.go
		p256_asm.go
		p256_asm_arm64.s
		p256_ordinv.go
		p384.go
		p521.go
    )
    
    GO_EMBED_PATTERN(p256_asm_table.bin)
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		nistec.go
		p224.go
		p224_sqrt.go
		p256_asm.go
		p256_asm_amd64.s
		p256_ordinv.go
		p384.go
		p521.go
    )
    
    GO_EMBED_PATTERN(p256_asm_table.bin)
ELSEIF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		nistec.go
		p224.go
		p224_sqrt.go
		p256_asm.go
		p256_asm_arm64.s
		p256_ordinv.go
		p384.go
		p521.go
    )
    
    GO_EMBED_PATTERN(p256_asm_table.bin)
ELSEIF (OS_DARWIN AND ARCH_AARCH64)
    SRCS(
		nistec.go
		p224.go
		p224_sqrt.go
		p256_asm.go
		p256_asm_arm64.s
		p256_ordinv.go
		p384.go
		p521.go
    )
    
    GO_EMBED_PATTERN(p256_asm_table.bin)
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		nistec.go
		p224.go
		p224_sqrt.go
		p256_asm.go
		p256_asm_amd64.s
		p256_ordinv.go
		p384.go
		p521.go
    )
    
    GO_EMBED_PATTERN(p256_asm_table.bin)
ELSEIF (OS_WINDOWS AND ARCH_ARM64)
    SRCS(
		nistec.go
		p224.go
		p224_sqrt.go
		p256_asm.go
		p256_asm_arm64.s
		p256_ordinv.go
		p384.go
		p521.go
    )
    
    GO_EMBED_PATTERN(p256_asm_table.bin)
ELSEIF (OS_WINDOWS AND ARCH_AARCH64)
    SRCS(
		nistec.go
		p224.go
		p224_sqrt.go
		p256_asm.go
		p256_asm_arm64.s
		p256_ordinv.go
		p384.go
		p521.go
    )
    
    GO_EMBED_PATTERN(p256_asm_table.bin)
ENDIF()
END()


RECURSE(
	fiat
)
