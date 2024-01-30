GO_LIBRARY()
IF (FALSE)
    MESSAGE(FATAL this shall never happen)

ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		crypto.go
    )
ELSEIF (OS_LINUX AND ARCH_ARM64)
    SRCS(
		crypto.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		crypto.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		crypto.go
    )
ELSEIF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		crypto.go
    )
ELSEIF (OS_DARWIN AND ARCH_AARCH64)
    SRCS(
		crypto.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		crypto.go
    )
ELSEIF (OS_WINDOWS AND ARCH_ARM64)
    SRCS(
		crypto.go
    )
ELSEIF (OS_WINDOWS AND ARCH_AARCH64)
    SRCS(
		crypto.go
    )
ENDIF()
END()


RECURSE(
	aes
	boring
	cipher
	des
	dsa
	ecdh
	ecdsa
	ed25519
	elliptic
	hmac
	internal
	md5
	rand
	rc4
	rsa
	sha1
	sha256
	sha512
	subtle
	tls
	x509
)
