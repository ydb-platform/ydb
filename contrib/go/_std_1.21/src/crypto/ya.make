GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64 OR OS_DARWIN AND ARCH_X86_64 OR OS_LINUX AND ARCH_AARCH64 OR OS_LINUX AND ARCH_X86_64 OR OS_WINDOWS AND ARCH_X86_64)
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
