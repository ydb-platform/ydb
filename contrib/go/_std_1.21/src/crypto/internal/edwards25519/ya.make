GO_LIBRARY()
IF (FALSE)
    MESSAGE(FATAL this shall never happen)

ELSEIF (OS_LINUX AND ARCH_X86_64)
    SRCS(
		doc.go
		edwards25519.go
		scalar.go
		scalar_fiat.go
		scalarmult.go
		tables.go
    )
ELSEIF (OS_LINUX AND ARCH_ARM64)
    SRCS(
		doc.go
		edwards25519.go
		scalar.go
		scalar_fiat.go
		scalarmult.go
		tables.go
    )
ELSEIF (OS_LINUX AND ARCH_AARCH64)
    SRCS(
		doc.go
		edwards25519.go
		scalar.go
		scalar_fiat.go
		scalarmult.go
		tables.go
    )
ELSEIF (OS_DARWIN AND ARCH_X86_64)
    SRCS(
		doc.go
		edwards25519.go
		scalar.go
		scalar_fiat.go
		scalarmult.go
		tables.go
    )
ELSEIF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
		doc.go
		edwards25519.go
		scalar.go
		scalar_fiat.go
		scalarmult.go
		tables.go
    )
ELSEIF (OS_DARWIN AND ARCH_AARCH64)
    SRCS(
		doc.go
		edwards25519.go
		scalar.go
		scalar_fiat.go
		scalarmult.go
		tables.go
    )
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		doc.go
		edwards25519.go
		scalar.go
		scalar_fiat.go
		scalarmult.go
		tables.go
    )
ELSEIF (OS_WINDOWS AND ARCH_ARM64)
    SRCS(
		doc.go
		edwards25519.go
		scalar.go
		scalar_fiat.go
		scalarmult.go
		tables.go
    )
ELSEIF (OS_WINDOWS AND ARCH_AARCH64)
    SRCS(
		doc.go
		edwards25519.go
		scalar.go
		scalar_fiat.go
		scalarmult.go
		tables.go
    )
ENDIF()
END()


RECURSE(
	field
)
