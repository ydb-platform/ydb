GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64 OR OS_DARWIN AND ARCH_X86_64 OR OS_LINUX AND ARCH_AARCH64 OR OS_LINUX AND ARCH_X86_64 OR OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		attr_string.go
		buf.go
		class_string.go
		const.go
		entry.go
		line.go
		open.go
		tag_string.go
		type.go
		typeunit.go
		unit.go
    )
ENDIF()
END()
