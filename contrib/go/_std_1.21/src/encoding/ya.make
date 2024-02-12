GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		encoding.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		encoding.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		encoding.go
    )
ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		encoding.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		encoding.go
    )
ENDIF()
END()


RECURSE(
	ascii85
	asn1
	base32
	base64
	binary
	csv
	gob
	hex
	json
	pem
	xml
)
