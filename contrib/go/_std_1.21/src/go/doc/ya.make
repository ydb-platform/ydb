GO_LIBRARY()
IF (OS_DARWIN AND ARCH_ARM64 OR OS_DARWIN AND ARCH_X86_64 OR OS_LINUX AND ARCH_AARCH64 OR OS_LINUX AND ARCH_X86_64 OR OS_WINDOWS AND ARCH_X86_64)
    SRCS(
		comment.go
		doc.go
		example.go
		exports.go
		filter.go
		reader.go
		synopsis.go
    )
ENDIF()
END()


RECURSE(
	comment
)
