GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64 OR OS_DARWIN AND ARCH_X86_64 OR OS_LINUX AND ARCH_AARCH64 OR OS_LINUX AND ARCH_X86_64 OR OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		dec_helpers.go
		decode.go
		decoder.go
		doc.go
		enc_helpers.go
		encode.go
		encoder.go
		error.go
		type.go
    )
ENDIF()
END()
