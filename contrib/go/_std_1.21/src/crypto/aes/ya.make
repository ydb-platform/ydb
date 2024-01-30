GO_LIBRARY()
IF (FALSE)
    MESSAGE(FATAL this shall never happen)

ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		aes_gcm.go
		asm_amd64.s
		block.go
		cipher.go
		cipher_asm.go
		const.go
		gcm_amd64.s
		modes.go
    )
ELSEIF (OS_LINUX AND ARCH_ARM64)
    SRCS(
		aes_gcm.go
		asm_arm64.s
		block.go
		cipher.go
		cipher_asm.go
		const.go
		gcm_arm64.s
		modes.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		aes_gcm.go
		asm_arm64.s
		block.go
		cipher.go
		cipher_asm.go
		const.go
		gcm_arm64.s
		modes.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		aes_gcm.go
		asm_amd64.s
		block.go
		cipher.go
		cipher_asm.go
		const.go
		gcm_amd64.s
		modes.go
    )
ELSEIF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		aes_gcm.go
		asm_arm64.s
		block.go
		cipher.go
		cipher_asm.go
		const.go
		gcm_arm64.s
		modes.go
    )
ELSEIF (OS_DARWIN AND ARCH_AARCH64)
    SRCS(
		aes_gcm.go
		asm_arm64.s
		block.go
		cipher.go
		cipher_asm.go
		const.go
		gcm_arm64.s
		modes.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		aes_gcm.go
		asm_amd64.s
		block.go
		cipher.go
		cipher_asm.go
		const.go
		gcm_amd64.s
		modes.go
    )
ELSEIF (OS_WINDOWS AND ARCH_ARM64)
    SRCS(
		aes_gcm.go
		asm_arm64.s
		block.go
		cipher.go
		cipher_asm.go
		const.go
		gcm_arm64.s
		modes.go
    )
ELSEIF (OS_WINDOWS AND ARCH_AARCH64)
    SRCS(
		aes_gcm.go
		asm_arm64.s
		block.go
		cipher.go
		cipher_asm.go
		const.go
		gcm_arm64.s
		modes.go
    )
ENDIF()
END()
