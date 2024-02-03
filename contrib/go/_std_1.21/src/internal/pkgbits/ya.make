GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64 OR OS_DARWIN AND ARCH_X86_64 OR OS_LINUX AND ARCH_AARCH64 OR OS_LINUX AND ARCH_X86_64 OR OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		codes.go
		decoder.go
		doc.go
		encoder.go
		flags.go
		reloc.go
		support.go
		sync.go
		syncmarker_string.go
    )
ENDIF()
END()
